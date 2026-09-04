package transformer

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const SchemaVersion = "1.0.0"

// Record is the normalized lake/warehouse shape.
//
// Envelope (meta + user) is intentional: future sources (on-chain activity,
// device telemetry, KYC vendors) can share the same meta contract while the
// payload under "user" evolves behind schema_version.
type Record struct {
	Meta Meta `json:"meta"`
	User User `json:"user"`
}

type Meta struct {
	SchemaVersion string `json:"schema_version"`
	Source        string `json:"source"`
	IngestedAt    string `json:"ingested_at"`
	BatchID       string `json:"batch_id"`
	APIVersion    string `json:"api_version,omitempty"`
}

type User struct {
	SourceUUID   string   `json:"source_uuid"`
	Username     string   `json:"username"`
	Email        string   `json:"email"`
	Gender       string   `json:"gender"`
	Nationality  string   `json:"nationality"`
	Name         Name     `json:"name"`
	Location     Location `json:"location"`
	BirthAt      string   `json:"birth_at"`
	RegisteredAt string   `json:"registered_at"`
	Contact      Contact  `json:"contact"`
	AvatarURL    string   `json:"avatar_url"`
}

type Name struct {
	Title       string `json:"title"`
	First       string `json:"first"`
	Last        string `json:"last"`
	DisplayName string `json:"display_name"`
}

type Location struct {
	Street     string   `json:"street"`
	City       string   `json:"city"`
	State      string   `json:"state"`
	Country    string   `json:"country"`
	PostalCode string   `json:"postal_code"`
	Latitude   *float64 `json:"latitude,omitempty"`
	Longitude  *float64 `json:"longitude,omitempty"`
	UTCOffset  string   `json:"utc_offset"`
}

type Contact struct {
	Phone string `json:"phone"`
	Cell  string `json:"cell"`
}

// Dropped from the raw Random User payload (and why):
//   - login.password / salt / md5 / sha1 / sha256 — credentials, never land in a lake
//   - id (SSN / national id) — high-risk PII with no analytics value here
//   - picture.large / picture.medium — keep thumbnail only
//   - location.timezone.description — offset is the durable field
type rawUser struct {
	Gender string `json:"gender"`
	Name   struct {
		Title string `json:"title"`
		First string `json:"first"`
		Last  string `json:"last"`
	} `json:"name"`
	Location struct {
		Street struct {
			Number int    `json:"number"`
			Name   string `json:"name"`
		} `json:"street"`
		City        string          `json:"city"`
		State       string          `json:"state"`
		Country     string          `json:"country"`
		Postcode    json.RawMessage `json:"postcode"`
		Coordinates struct {
			Latitude  string `json:"latitude"`
			Longitude string `json:"longitude"`
		} `json:"coordinates"`
		Timezone struct {
			Offset string `json:"offset"`
		} `json:"timezone"`
	} `json:"location"`
	Email string `json:"email"`
	Login struct {
		UUID     string `json:"uuid"`
		Username string `json:"username"`
	} `json:"login"`
	DOB struct {
		Date string `json:"date"`
	} `json:"dob"`
	Registered struct {
		Date string `json:"date"`
	} `json:"registered"`
	Phone   string `json:"phone"`
	Cell    string `json:"cell"`
	Picture struct {
		Thumbnail string `json:"thumbnail"`
	} `json:"picture"`
	Nat string `json:"nat"`
}

type Result struct {
	Record Record
	Raw    json.RawMessage
	Err    error
}

func TransformAll(raw []json.RawMessage, source, apiVersion, schemaVersion, batchID string, ingestedAt time.Time) []Result {
	out := make([]Result, 0, len(raw))
	for i, item := range raw {
		rec, err := Transform(item, source, apiVersion, schemaVersion, batchID, ingestedAt)
		if err != nil {
			out = append(out, Result{Err: fmt.Errorf("record %d: %w", i, err), Raw: item})
			continue
		}
		out = append(out, Result{Record: rec})
	}
	return out
}

func Transform(raw json.RawMessage, source, apiVersion, schemaVersion, batchID string, ingestedAt time.Time) (Record, error) {
	var u rawUser
	if err := json.Unmarshal(raw, &u); err != nil {
		return Record{}, fmt.Errorf("unmarshal: %w", err)
	}
	if u.Login.UUID == "" {
		return Record{}, fmt.Errorf("missing login.uuid (natural key)")
	}
	if _, err := uuid.Parse(u.Login.UUID); err != nil {
		return Record{}, fmt.Errorf("login.uuid: %w", err)
	}
	if schemaVersion == "" {
		schemaVersion = SchemaVersion
	}

	birth, err := toUTCISO(u.DOB.Date)
	if err != nil {
		return Record{}, fmt.Errorf("dob: %w", err)
	}
	registered, err := toUTCISO(u.Registered.Date)
	if err != nil {
		return Record{}, fmt.Errorf("registered: %w", err)
	}

	lat, err := parseCoord("latitude", u.Location.Coordinates.Latitude)
	if err != nil {
		return Record{}, err
	}
	lon, err := parseCoord("longitude", u.Location.Coordinates.Longitude)
	if err != nil {
		return Record{}, err
	}

	display := strings.TrimSpace(u.Name.First + " " + u.Name.Last)
	street := strings.TrimSpace(fmt.Sprintf("%d %s", u.Location.Street.Number, u.Location.Street.Name))

	return Record{
		Meta: Meta{
			SchemaVersion: schemaVersion,
			Source:        source,
			IngestedAt:    ingestedAt.UTC().Format(time.RFC3339Nano),
			BatchID:       batchID,
			APIVersion:    apiVersion,
		},
		User: User{
			SourceUUID:  u.Login.UUID,
			Username:    u.Login.Username,
			Email:       strings.ToLower(strings.TrimSpace(u.Email)),
			Gender:      u.Gender,
			Nationality: u.Nat,
			Name: Name{
				Title:       u.Name.Title,
				First:       u.Name.First,
				Last:        u.Name.Last,
				DisplayName: display,
			},
			Location: Location{
				Street:     street,
				City:       u.Location.City,
				State:      u.Location.State,
				Country:    u.Location.Country,
				PostalCode: stringifyPostcode(u.Location.Postcode),
				Latitude:   lat,
				Longitude:  lon,
				UTCOffset:  u.Location.Timezone.Offset,
			},
			BirthAt:      birth,
			RegisteredAt: registered,
			Contact: Contact{
				Phone: digits(u.Phone),
				Cell:  digits(u.Cell),
			},
			AvatarURL: u.Picture.Thumbnail,
		},
	}, nil
}

func toUTCISO(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty timestamp")
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999Z07:00",
		"2006-01-02T15:04:05Z07:00",
	}
	var parsed time.Time
	var err error
	for _, layout := range layouts {
		parsed, err = time.Parse(layout, s)
		if err == nil {
			return parsed.UTC().Format(time.RFC3339Nano), nil
		}
	}
	return "", fmt.Errorf("unsupported timestamp %q", s)
}

func parseCoord(name, raw string) (*float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, fmt.Errorf("%s: non-finite value %q", name, raw)
	}
	return &v, nil
}

func stringifyPostcode(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return asString
	}
	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return asNumber.String()
	}
	return strings.Trim(string(raw), `"`)
}

func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
