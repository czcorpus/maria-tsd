// Copyright 2024 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2024 Institute of the Czech National Corpus,
//                Faculty of Arts, Charles University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package data

import (
	"math"
	"time"
)

type Point struct {
	name      string
	tags      map[string]string
	value     float64
	timestamp time.Time
}

func (p *Point) Tag(name string) string {
	v, _ := p.tags[name]
	return v
}

func (p *Point) Tags() map[string]string {
	if p.tags != nil {
		return p.tags
	}
	return map[string]string{}
}

func (p *Point) SetTag(name, value string) *Point {
	if p.tags == nil {
		p.tags = make(map[string]string)
	}
	p.tags[name] = value
	return p
}

func (p *Point) SetTags(tags map[string]string) *Point {
	if p.tags == nil {
		p.tags = make(map[string]string)
	}
	for k, v := range tags {
		p.tags[k] = v
	}
	return p
}

func (p *Point) HasTags() bool {
	return p.tags != nil && len(p.tags) > 0
}

func (p *Point) Time() time.Time {
	return p.timestamp
}

func (p *Point) SetTime(t time.Time) *Point {
	p.timestamp = t
	return p
}

func (p *Point) Name() string {
	return p.name
}

func (p *Point) Value() float64 {
	return p.value
}

func (p *Point) SetValue(v float64) *Point {
	p.value = v
	return p
}

func (p *Point) SetIntValue(v int64) *Point {
	p.value = float64(v)
	return p
}

func (p *Point) IntValue() int64 {
	return int64(math.RoundToEven(p.value))
}

func NewPoint(name string) *Point {
	return &Point{
		name: name,
	}
}
