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

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/czcorpus/cnc-gokit/fs"
	"github.com/czcorpus/maria-tsd/data"
	"github.com/rs/zerolog/log"
)

func determineConfigPath(confPath *string) {
	if *confPath != "" {
		return
	}
	exe, err := os.Executable()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to determine mtsd executable location")
		return
	}
	localConf := filepath.Join(filepath.Dir(exe), "conf.json")
	isF, err := fs.IsFile(localConf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to determine mtsd executable location")
		return
	}
	if isF {
		*confPath = localConf
		return
	}
	log.Fatal().Msg("cannot find suitable config")
}

func parseCmdFlags(v string) map[string]string {
	ans := make(map[string]string)
	for _, pair := range strings.Split(v, ",") {
		kv := strings.Split(pair, ":")
		ans[kv[0]] = kv[1]
	}
	return ans
}

func main() {
	confPath := flag.String("conf", "", "path to a config")
	tags := flag.String("tags", "", "list of tags like this: `k1:v1,k2:v2,...,kN:vN`")
	flag.Parse()
	determineConfigPath(confPath)
	action := flag.Arg(0)
	switch action {
	case "read":
		cnf := LoadConfig(*confPath)
		dbc, err := data.NewMeasurementGroup(
			cnf.DB,
			cnf.TimezoneLocation(),
			flag.Arg(1),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open measurement group")
			return
		}
		fmt.Println("db is ", dbc)
	case "write":
		cnf := LoadConfig(*confPath)
		dbc, err := data.NewMeasurementGroup(
			cnf.DB,
			cnf.TimezoneLocation(),
			flag.Arg(1),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open measurement group")
			return
		}
		v, err := strconv.ParseFloat(flag.Arg(3), 32)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to parse number value")
			return
		}
		p := data.NewPoint(flag.Arg(2)).SetValue(v)
		if *tags != "" {
			p.SetTags(parseCmdFlags(*tags))
		}
		err = dbc.WriteSync(p)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to parse number value")
			return
		}
		fmt.Println("write via ", dbc)
	}

}
