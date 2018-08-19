package kong

import (
	"bytes"
	"strconv"
)

// Route represents a Route in Kong.
// Read https://getkong.org/docs/0.13.x/admin-api/#Route-object
type Route struct {
	CreatedAt     *int      `json:"created_at,omitempty"`
	Hosts         []*string `json:"hosts,omitempty"`
	ID            *string   `json:"id,omitempty"`
	Methods       []*string `json:"methods,omitempty"`
	Paths         []*string `json:"paths,omitempty"`
	PreserveHost  *bool     `json:"preserve_host,omitempty"`
	Protocols     []*string `json:"protocols,omitempty"`
	RegexPriority *int      `json:"regex_priority,omitempty"`
	Service       *Service  `json:"service,omitempty"`
	StripPath     *bool     `json:"strip_path,omitempty"`
	UpdatedAt     *int      `json:"updated_at,omitempty"`
}

// Valid checks if all the fields in Route are valid.
func (r *Route) Valid() bool {
	if len(r.Methods) == 0 && len(r.Paths) == 0 && len(r.Hosts) == 0 {
		return false
	}
	return true
}

func (r *Route) String() string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	buf.WriteByte(' ')
	if r.ID == nil {
		buf.WriteString("nil")
	} else {
		buf.WriteString(*r.ID)
	}
	buf.WriteByte(' ')
	buf.WriteString(stringArrayToString(r.Methods))
	buf.WriteByte(' ')
	buf.WriteString(stringArrayToString(r.Hosts))
	buf.WriteByte(' ')
	buf.WriteString(stringArrayToString(r.Paths))
	buf.WriteByte(' ')
	if r.PreserveHost == nil {
		buf.WriteString("nil")
	} else {
		buf.WriteString(strconv.FormatBool(*r.PreserveHost))
	}
	buf.WriteByte(' ')
	if r.StripPath == nil {
		buf.WriteString("nil")
	} else {
		buf.WriteString(strconv.FormatBool(*r.StripPath))
	}
	buf.WriteByte(' ')
	if r.RegexPriority == nil {
		buf.WriteString("nil")
	} else {
		buf.WriteString(strconv.Itoa(*r.RegexPriority))
	}
	buf.WriteByte(' ')
	if r.Service == nil || r.Service.ID == nil {
		buf.WriteString("nil")
	} else {
		buf.WriteString(*r.Service.ID)
	}
	buf.WriteByte(' ')
	buf.WriteByte(']')
	return buf.String()
}
