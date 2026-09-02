// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrgen

import (
	"fmt"
	"strconv"
	"strings"
)

// tagOpts is the parsed form of one `arrow:"..."` struct tag. It mirrors the
// unexported tagOpts in arreflect; keep the two in sync.
type tagOpts struct {
	Name             string
	Skip             bool
	Dict             bool
	View             bool
	REE              bool
	Large            bool
	DecimalPrecision int32
	DecimalScale     int32
	HasDecimalOpts   bool
	Temporal         string // "", "timestamp", "date32", "date64", "time32", "time64"
}

// parseTag parses the value of an `arrow` struct tag. Unlike arreflect, which
// defers diagnostics, an unusable tag is an error.
func parseTag(tag string) (tagOpts, error) {
	if tag == "-" {
		return tagOpts{Skip: true}, nil
	}

	name, rest, _ := strings.Cut(tag, ",")
	opts := tagOpts{Name: name}
	if rest == "" {
		return opts, nil
	}
	for _, token := range splitTagTokens(rest) {
		if err := applyTagToken(&opts, token); err != nil {
			return tagOpts{}, err
		}
	}
	return opts, nil
}

// splitTagTokens splits the option list on commas outside parentheses, so
// decimal(18,2) stays one token.
func splitTagTokens(rest string) []string {
	var tokens []string
	depth, start := 0, 0
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				tokens = append(tokens, strings.TrimSpace(rest[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(rest) {
		tokens = append(tokens, strings.TrimSpace(rest[start:]))
	}
	return tokens
}

func applyTagToken(opts *tagOpts, token string) error {
	if strings.HasPrefix(token, "decimal(") && strings.HasSuffix(token, ")") {
		return parseDecimalOpt(opts, token)
	}
	switch token {
	case "dict":
		opts.Dict = true
	case "view":
		opts.View = true
	case "ree":
		opts.REE = true
	case "large":
		opts.Large = true
	case "date32", "date64", "time32", "time64", "timestamp":
		opts.Temporal = token
	default:
		return fmt.Errorf("unknown option %q", token)
	}
	return nil
}

func parseDecimalOpt(opts *tagOpts, token string) error {
	inner := strings.TrimSuffix(strings.TrimPrefix(token, "decimal("), ")")
	parts := strings.SplitN(inner, ",", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid decimal tag %q: expected decimal(precision,scale)", token)
	}
	p, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid decimal tag %q: precision %q is not an integer", token, strings.TrimSpace(parts[0]))
	}
	s, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid decimal tag %q: scale %q is not an integer", token, strings.TrimSpace(parts[1]))
	}
	opts.HasDecimalOpts = true
	opts.DecimalPrecision = int32(p)
	opts.DecimalScale = int32(s)
	return nil
}

// validate rejects the option combinations arreflect also rejects.
func (o tagOpts) validate() error {
	if o.REE {
		return fmt.Errorf("ree is not supported on a struct field; use arreflect.FromSlice with WithREE at the top level")
	}
	n := 0
	for _, set := range []bool{o.Dict, o.View, o.REE} {
		if set {
			n++
		}
	}
	if n > 1 {
		return fmt.Errorf("conflicting options: at most one of dict, view, ree may be set")
	}
	return nil
}
