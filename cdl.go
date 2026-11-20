package agent

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

// CDL is the text-only setup and evaluation configuration produced by Control.
// System is the evaluation's system attribute, typically a numeric ID in v3.
type CDL struct {
	System     string
	Setup      map[string]string
	Evaluation map[string]string
}

// ParseCDL validates the v1 document shape and preserves exact parameter text.
func ParseCDL(raw string) (CDL, error) {
	var cdl CDL
	if len(raw) > maxResponseBytes {
		return cdl, errors.New("CDL exceeds 4 MiB")
	}
	decoder := xml.NewDecoder(strings.NewReader(strings.TrimPrefix(raw, "\uFEFF")))
	var path []xml.Name
	var section map[string]string
	var value strings.Builder
	var rootSeen, xmlDeclaration bool
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return CDL{}, fmt.Errorf("parse CDL: %w", err)
		}
		switch t := token.(type) {
		case xml.Directive:
			return CDL{}, errors.New("XML directives are unsupported in CDL")
		case xml.ProcInst:
			if t.Target != "xml" || rootSeen || xmlDeclaration {
				return CDL{}, errors.New("unexpected XML processing instruction")
			}
			xmlDeclaration = true
		case xml.StartElement:
			if t.Name.Space != "" {
				return CDL{}, errors.New("CDL elements must have no namespace")
			}
			switch len(path) {
			case 0:
				if rootSeen || t.Name.Local != "chronos" || len(t.Attr) != 0 {
					return CDL{}, errors.New("CDL requires one <chronos> root")
				}
				rootSeen = true
			case 1:
				switch {
				case t.Name.Local == "setup" && cdl.Setup == nil && len(t.Attr) == 0:
					cdl.Setup = make(map[string]string)
					section = cdl.Setup
				case t.Name.Local == "evaluation" && cdl.Evaluation == nil && len(t.Attr) == 1 &&
					t.Attr[0].Name == (xml.Name{Local: "system"}) && strings.TrimSpace(t.Attr[0].Value) != "":
					cdl.System = t.Attr[0].Value
					cdl.Evaluation = make(map[string]string)
					section = cdl.Evaluation
				default:
					return CDL{}, errors.New("invalid or duplicate CDL section")
				}
			case 2:
				if _, exists := section[t.Name.Local]; exists || len(t.Attr) != 0 {
					return CDL{}, fmt.Errorf("invalid or duplicate CDL parameter %q", t.Name.Local)
				}
			default:
				return CDL{}, errors.New("nested CDL parameters are unsupported")
			}
			path = append(path, t.Name)
		case xml.CharData:
			if len(path) == 3 {
				value.Write(t)
			} else if len(strings.Trim(string(t), " \t\r\n")) != 0 {
				return CDL{}, errors.New("unexpected text outside a CDL parameter")
			}
		case xml.EndElement:
			if len(path) == 3 {
				section[t.Name.Local] = value.String()
				value.Reset()
			}
			path = path[:len(path)-1]
		}
	}
	if len(path) != 0 || cdl.Setup == nil || cdl.Evaluation == nil {
		return CDL{}, errors.New("CDL requires setup and evaluation sections")
	}
	return cdl, nil
}
