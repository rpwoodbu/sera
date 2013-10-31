package callsign_lookup

import (
	"appengine"
	"appengine/datastore"
	"appengine/user"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Member struct {
	Callsign string
	LastName string
	Name     string
	Street   string
	City     string
	State    string
	Zip      string
	//HomePhone       string
	League          string
	HomeRepeater    string
	DateJoined      string
	MemberType      string
	Status          string
	QuarterExpiring int
	YearExpiring    int
	//Email           string
}

const (
	CALL_HEADER             = "CALL"
	LAST_NAME_HEADER        = "LASTNAME"
	NAME_HEADER             = "NAME"
	STREET_HEADER           = "STREET"
	CITY_HEADER             = "CITY"
	STATE_HEADER            = "STATE"
	ZIP_HEADER              = "ZIP"
	HOME_PHONE_HEADER       = "HOMEPHONE"
	LEAGUE_HEADER           = "LEAGUE"
	HOME_REPEATER_HEADER    = "HOMERPT"
	DATE_JOINED_HEADER      = "DATEJOIN"
	MEMBER_TYPE_HEADER      = "MEMTYPE"
	STATUS_HEADER           = "STATUS"
	QUARTER_EXPIRING_HEADER = "QTREXP"
	YEAR_EXPIRING_HEADER    = "YEAREXP"
	EMAIL_HEADER            = "EMAIL"
)

var columnNames = map[string]bool{
	CALL_HEADER:             false,
	LAST_NAME_HEADER:        false,
	NAME_HEADER:             false,
	STREET_HEADER:           false,
	CITY_HEADER:             false,
	STATE_HEADER:            false,
	ZIP_HEADER:              false,
	HOME_PHONE_HEADER:       false,
	LEAGUE_HEADER:           false,
	HOME_REPEATER_HEADER:    false,
	DATE_JOINED_HEADER:      false,
	MEMBER_TYPE_HEADER:      false,
	STATUS_HEADER:           false,
	QUARTER_EXPIRING_HEADER: false,
	YEAR_EXPIRING_HEADER:    false,
	EMAIL_HEADER:            false,
}

const NUM_WRITERS = 50

const rootPage = `
<html>
	<body>
		<form action="/lookup" method="get">
			<div>Callsign: <input type="text" name="callsign"></input></div>
			<div><input type="submit" value="Lookup"></div>
		</form>
	</body>
</html>
`

const lookupPage = `
<html>
	<body>
		<h3>{{.Callsign}}</h3>
		<div>{{.Name}}</div>
		<div>{{.Street}}</div>
		<div>{{.City}}, {{.State}} {{.Zip}}</div>
		<div>Joined: {{.DateJoined}}</div>
		<div>Expires: Q{{.QuarterExpiring}} {{.YearExpiring}}</div>
	</body>
</html>
`

const lookupNotFoundPage = `
<html>
	<body>
		<div>Not Found.</div>
	</body>
</html>
`

const updatePage = `
<html>
	<body>
		<form enctype="multipart/form-data" method="post">
			<p>Hello, %v!</p>
			<p>Upload a new CSV file with callsigns.
			This will erase all data in favor of the new file.</p>
			<div><input type="file" name="csvfile"></input></div>
			<div><input type="submit" value="Update"></input></div>
		</form>
	</body>
</html>
`

func init() {
	http.HandleFunc("/", root)
	http.HandleFunc("/lookup", lookup)
	http.HandleFunc("/update", update)
}

var lookupTemplate = template.Must(template.New("lookup").Parse(lookupPage))

func root(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, rootPage)
}

func lookup(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	callsign := strings.ToUpper(r.FormValue("callsign"))
	format := strings.ToLower(r.FormValue("format"))

	key := datastore.NewKey(c, "Member", callsign, 0, nil)
	var member Member
	err := datastore.Get(c, key, &member)
	if err != nil {
		if format != "json" {
			fmt.Fprintf(w, lookupNotFoundPage)
			return
		}
		// Otherwise, continue and return an empty Member object.
	}

	if format == "json" {
		output, err := json.Marshal(member)
		if err != nil {
			fmt.Fprintf(w, lookupNotFoundPage)
			return
		}
		jsonp := r.FormValue("jsonp")
		if len(jsonp) > 0 {
			fmt.Fprintf(w, "%s(%s);", jsonp, output)
		} else {
			w.Write(output)
		}
		return
	}

	err = lookupTemplate.Execute(w, member)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func update(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	c.Debugf("update() entered")
	u := user.Current(c)
	if u == nil {
		url, err := user.LoginURL(c, r.URL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", url)
		w.WriteHeader(http.StatusFound)
		return
	}

	csvRaw, _, err := r.FormFile("csvfile")
	if err != nil {
		fmt.Fprintf(w, updatePage, u)
		return
	}

	c.Debugf("receiving file")

	// Create a map of all members. We will delete each member from this map as
	// we update their entries. Any remaining members should be purged from the
	// Datastore.
	existing := make(map[string]bool)
	q := datastore.NewQuery("Member").KeysOnly()
	c.Debugf("reading all keys")
	t := q.Run(c)
	for {
		key, err := t.Next(nil)
		if err == datastore.Done {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		existing[key.Encode()] = true
	}

	c.Debugf("keys all read")

	fmt.Fprintf(w, "<html><body>")

	csvReader := csv.NewReader(csvRaw)
	csvReader.TrailingComma = true
	csvReader.FieldsPerRecord = -1

	header, err := csvReader.Read()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	columns := make(map[string]int) // Map of column names to record offsets.
	for i, heading := range header {
		if _, ok := columnNames[heading]; ok {
			columns[heading] = i
		}
	}

	output := make(chan *Member)
	result := make(chan []error)
	c.Debugf("start writers")
	for i := 0; i < NUM_WRITERS; i++ {
		go write(c, output, result)
	}

	added, updated := 0, 0
	processed := make(map[string]bool)
	duplicates := make(map[string]bool)
	c.Debugf("begin writing")
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		var member Member
		member.Callsign = strings.ToUpper(strings.TrimSpace(record[columns[CALL_HEADER]]))
		member.LastName = strings.TrimSpace(record[columns[LAST_NAME_HEADER]])
		member.Name = strings.TrimSpace(record[columns[NAME_HEADER]])
		member.Street = strings.TrimSpace(record[columns[STREET_HEADER]])
		member.City = strings.TrimSpace(record[columns[CITY_HEADER]])
		member.State = strings.TrimSpace(record[columns[STATE_HEADER]])
		member.Zip = strings.TrimSpace(record[columns[ZIP_HEADER]])
		//member.HomePhone = strings.TrimSpace(record[columns[HOME_PHONE_HEADER]])
		member.League = strings.TrimSpace(record[columns[LEAGUE_HEADER]])
		member.HomeRepeater = strings.TrimSpace(record[columns[HOME_REPEATER_HEADER]])
		member.DateJoined = strings.TrimSpace(record[columns[DATE_JOINED_HEADER]])
		member.MemberType = strings.TrimSpace(record[columns[MEMBER_TYPE_HEADER]])
		member.Status = strings.TrimSpace(record[columns[STATUS_HEADER]])
		member.QuarterExpiring, err = strconv.Atoi(record[columns[QUARTER_EXPIRING_HEADER]])
		if err != nil {
			fmt.Fprintf(w, "<div>Cannot parse %v's Quarter Expiring as a number: <pre>%s</pre></div>",
				member.Callsign, record[columns[QUARTER_EXPIRING_HEADER]])
		}
		member.YearExpiring, err = strconv.Atoi(record[columns[YEAR_EXPIRING_HEADER]])
		if err != nil {
			fmt.Fprintf(w, "<div>Cannot parse %v's Year Expiring as a number: <pre>%s</pre></div>",
				member.Callsign, record[columns[YEAR_EXPIRING_HEADER]])
		}
		//member.Email = strings.TrimSpace(record[columns[EMAIL_HEADER]])

		key := datastore.NewKey(c, "Member", member.Callsign, 0, nil).Encode()
		_, ok := existing[key]
		if !ok {
			if _, ok := processed[member.Callsign]; ok {
				duplicates[member.Callsign] = true
				// Do not add a duplicate again; move on.
				continue
			}
			added++
			fmt.Fprintf(w, "<div>Adding %v</div>", member.Callsign)
		} else {
			updated++
		}

		output <- &member
		processed[member.Callsign] = true
		delete(existing, key)
	}

	c.Debugf("closing output")
	close(output)
	c.Debugf("waiting for results")
	for i := 0; i < NUM_WRITERS; i++ {
		errors := <-result
		if len(errors) > 0 {
			for _, err := range errors {
				fmt.Fprintf(w, "<div>Error: %v</div>", err)
			}
		}
	}

	// Now delete any members remaining in the map.
	c.Debugf("deleting keys")
	var keys []*datastore.Key
	for encodedKey := range existing {
		key, err := datastore.DecodeKey(encodedKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		keys = append(keys, key)
		fmt.Fprintf(w, "<div>Deleting %v</div>", key.StringID())
	}
	err = datastore.DeleteMulti(c, keys)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "<div>Added %d, updated %d, deleted %d.</div>",
		added, updated, len(existing))
	if len(duplicates) > 0 {
		fmt.Fprintf(w, "<div>Found %d duplicates:<ul>", len(duplicates))
		for callsign := range duplicates {
			fmt.Fprintf(w, "<li>%v", callsign)
		}
		fmt.Fprintf(w, "</ul>")
	}
}

func write(c appengine.Context, output chan *Member, result chan []error) {
	var errors []error

	for member := range output {
		key := datastore.NewKey(c, "Member", member.Callsign, 0, nil)
		_, err := datastore.Put(c, key, member)
		if err != nil {
			errors = append(errors, err)
		}
	}

	result <- errors
}
