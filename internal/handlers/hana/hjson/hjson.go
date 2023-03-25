// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package hjson provides converters from/to jsonb with some extensions for built-in and `types` types.
//
// See contributing guidelines and documentation for package `types` for details.
//
// # Mapping
//
// HJSON uses schema to map values to data types.
// Schema is stored in the `&s` field of the document and contains information about the fields.
// A document with schema looks like this:
//
//	{
//	   "&s": {
//	     "&k": ["field1", "field2", ...],
//	     "p": {
//	       "field1": {<schema>},
//	       "field2": {<schema>},
//	       ...
//	   }
//	   "field1": <json representation>,
//	   "field2": <json representation>,
//	   ...
//	}
//
// Composite types
//
//	Alias      types package    hjson package        hjson schema                                            JSON representation
//
//	object     *types.Document  *hjson.documentType  {"t":"object", "&s": {"&k":[<keys>], "p":{<properties>}} JSON object
//	array      *types.Array     *hjson.arrayType     {"t":"array", "i": [<item 1>, <item 2>]}                JSON array
//
// Scalar types
//
//		Alias      types package   hjson package         hjson schema                         JSON representation
//
//		double     float64         *hjson.doubleType    {"t":"double"}                        JSON number
//		string     string          *hjson.stringType    {"t":"string"}                        JSON string
//		binData    types.Binary    *hjson.binaryType    {"t":"binData",
//		                                                 "s":<subtype number>}                "<base 64 string>"
//		objectId   types.ObjectID  *hjson.objectIDType  {"t":"objectId"}                      "<ObjectID as 24 character hex string>"
//		bool       bool            *hjson.boolType      {"t":"bool"}                          JSON true / false values
//		date       time.Time       *hjson.dateTimeType  {"t":"date"}   						  milliseconds since epoch as JSON number
//		null       types.NullType  *hjson.nullType      {"t":"null"}                          JSON null
//		regex      types.Regex     *hjson.regexType     {"t":"regex",
//	                                                	 "o": "<string w/o terminating 0x0>"} "<string w/o terminating 0x0>"
//		int        int32           *hjson.int32Type     {"t":"int"}   			              JSON number
//		timestamp  types.Timestamp *hjson.timestampType {"t":"timestamp"}                     JSON number
//		long       int64           *hjson.int64Type     {"t":"long"}                          JSON number
//
//nolint:lll // for readability
//nolint:dupword // false positive
package hjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/AlekSi/pointer"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

// hjsontype is a type that can be marshaled from/to hjson.
type hjsontype interface {
	hjsontype() // seal for go-sumtype

	json.Marshaler
}

//go-sumtype:decl hjsontype

// checkConsumed returns error if decoder or reader have buffered or unread data.
func checkConsumed(dec *json.Decoder, r *bytes.Reader) error {
	if dr := dec.Buffered().(*bytes.Reader); dr.Len() != 0 {
		b, _ := io.ReadAll(dr)

		if l := len(b); l != 0 {
			return lazyerrors.Errorf("%d bytes remains in the decoder: %s", l, b)
		}
	}

	if l := r.Len(); l != 0 {
		b, _ := io.ReadAll(r)
		return lazyerrors.Errorf("%d bytes remains in the reader: %s", l, b)
	}

	return nil
}

// fromHJSON converts hjsontype value to matching built-in or types' package value.
func fromHJSON(v hjsontype) any {
	switch v := v.(type) {
	case *documentType:
		return pointer.To(types.Document(*v))
	case *arrayType:
		return pointer.To(types.Array(*v))
	case *stringType:
		return string(*v)
	}

	panic(fmt.Sprintf("not reached: %T", v)) // for go-sumtype to work
}

// toHJSON converts built-in or types' package value to hjsontype value.
func toHJSON(v any) hjsontype {
	switch v := v.(type) {
	case *types.Document:
		return pointer.To(documentType(*v))
	case *types.Array:
		return pointer.To(arrayType(*v))
	case string:
		return pointer.To(stringType(v))
	}

	panic(fmt.Sprintf("not reached: %T", v)) // for go-sumtype to work
}

// Unmarshal decodes the top-level document.
// It decodes document's schema from the `&s` field and uses it to decode the data of the document.
func Unmarshal(data []byte) (*types.Document, error) {
	var v map[string]json.RawMessage
	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)

	err := dec.Decode(&v)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err = checkConsumed(dec, r); err != nil {
		return nil, lazyerrors.Error(err)
	}

	// decode schema from the &s field of the document
	jsch, ok := v["&s"]
	if !ok {
		return nil, lazyerrors.Errorf("schema is not set")
	}

	var sch schema
	r = bytes.NewReader(jsch)
	dec = json.NewDecoder(r)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&sch); err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err := checkConsumed(dec, r); err != nil {
		return nil, lazyerrors.Error(err)
	}

	delete(v, "&s")

	// decode data from the rest of the document using the schema
	if len(sch.Keys) != len(v) {
		return nil, lazyerrors.Errorf(
			"hjson.Unmarshal: the data must have the same number of schema keys and document fields (keys: %d, fields: %d)",
			len(sch.Keys), len(v),
		)
	}

	d := must.NotFail(types.NewDocument())

	for _, key := range sch.Keys {
		b, ok := v[key]

		if !ok {
			return nil, lazyerrors.Errorf("hjson.Unmarshal: missing key %q", key)
		}

		v, err := unmarshalSingleValue(b, sch.Properties[key])
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		d.Set(key, v)
	}

	return d, nil
}

// unmarshalSingleValue decodes the given hjson-encoded data element by the given schema.
func unmarshalSingleValue(data []byte, sch *elem) (any, error) {
	if bytes.Equal(data, []byte("null")) {
		return fromHJSON(new(nullType)), nil
	}

	if sch == nil {
		return nil, lazyerrors.Errorf("schema is not set")
	}

	var v json.RawMessage
	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)

	err := dec.Decode(&v)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err := checkConsumed(dec, r); err != nil {
		return nil, lazyerrors.Error(err)
	}

	var res hjsontype

	switch sch.Type {
	case elemTypeObject:
		if sch.Schema == nil {
			return nil, lazyerrors.Errorf("hjson.unmarshalSingleValue: schema is not set")
		}

		var d documentType
		err = d.UnmarshalJSONWithSchema(data, sch.Schema)
		res = &d
	case elemTypeString:
		var s stringType
		err = s.UnmarshalJSON(data)
		res = &s
	default:
		return nil, lazyerrors.Errorf("hjson.unmarshalSingleValue: unhandled type %q", sch.Type)
	}

	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return fromHJSON(res), nil
}

// Marshal encodes the given document and set its schema in the field &s.
// Use it when you need to encode a document with schema, for example, when you want to store it in a database.
func Marshal(d *types.Document) ([]byte, error) {
	if d == nil {
		panic("v is nil")
	}

	schema, err := marshalSchemaForDoc(d)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	buf.WriteString(`{"&s":`)
	buf.Write(schema)

	keys := d.Keys()
	values := d.Values()

	for i, key := range keys {
		buf.WriteByte(',')
		buf.WriteString(`"`)
		buf.WriteString(key)
		buf.WriteString(`":`)

		b, err := toHJSON(values[i]).MarshalJSON()
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// MarshalSingleValue encodes given built-in or types' package value into hjson.
// Use it when you need to encode a single value, for example in a where clause.
func MarshalSingleValue(v any) ([]byte, error) {
	if v == nil {
		panic("v is nil")
	}

	b, err := toHJSON(v).MarshalJSON()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return b, nil
}
