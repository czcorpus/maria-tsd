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
	"database/sql"
	"fmt"
	"time"

	"github.com/czcorpus/maria-tsd/db"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

type MeasurementGroup struct {
	db       *sql.DB
	dbName   string
	location *time.Location
	name     string
}

func (mt *MeasurementGroup) mkTable0Name() string {
	return fmt.Sprintf("%s_tsd", mt.name)
}

func (mt *MeasurementGroup) mkTable1Name() string {
	return fmt.Sprintf("%s_tag", mt.name)
}

func (mt *MeasurementGroup) mkViewName() string {
	return fmt.Sprintf("%s_fullview", mt.name)
}

func (mt *MeasurementGroup) CreateTablesIfNone() error {
	tExists, err := mt.tableExists(mt.mkTable0Name())
	if err != nil {
		return fmt.Errorf("failed to test table existence: %w", err)
	}
	if !tExists {
		log.Debug().Msg("creating new table")
		sql0 := fmt.Sprintf(
			"CREATE TABLE %s ("+
				"id INT auto_increment PRIMARY KEY, "+
				"dt DATETIME NOT NULL, "+
				"name varchar(100) NOT NULL, "+
				"value float NOT NULL )",
			mt.mkTable0Name(),
		)
		if _, err := mt.db.Exec(sql0); err != nil {
			return fmt.Errorf("failed to create *_tsd table: %w", err)
		}
	}
	tExists, err = mt.tableExists(mt.mkTable1Name())
	if err != nil {
		return fmt.Errorf("failed to test table existence: %w", err)
	}
	if !tExists {
		sql1 := fmt.Sprintf(
			"CREATE TABLE %s ("+
				"id INT NOT NULL auto_increment PRIMARY KEY, "+
				"name VARCHAR(100) NOT NULL, "+
				"value VARCHAR(100) NOT NULL, "+
				"point_id INT NOT NULL REFERENCES %s(id) )",
			mt.mkTable1Name(),
			mt.mkTable0Name(),
		)
		if _, err := mt.db.Exec(sql1); err != nil {
			return fmt.Errorf("failed to create *_tag table: %w", err)
		}
	}
	vExists, err := mt.viewExists(mt.mkViewName())
	if err != nil {
		return fmt.Errorf("failed to test view existence: %w", err)
	}
	if !vExists {
		sql2 := fmt.Sprintf(
			"CREATE VIEW %s AS "+
				"SELECT t.*, GROUP_CONCAT(CONCAT(g.name, ':', g.value)) AS tags "+
				"FROM %s AS t LEFT JOIN %s AS g ON t.id = g.point_id "+
				"GROUP BY t.id ",
			mt.mkViewName(), mt.mkTable0Name(), mt.mkTable1Name())
		if _, err := mt.db.Exec(sql2); err != nil {
			return fmt.Errorf("failed to create *_fullview table: %w", err)
		}
	}
	return nil
}

func (mt *MeasurementGroup) viewExists(tableName string) (bool, error) {
	sql0 := "SELECT table_name " +
		"FROM information_schema.views " +
		"WHERE table_schema = ? " +
		"AND table_name = ? " +
		"LIMIT 1"
	row := mt.db.QueryRow(sql0, mt.dbName, tableName)
	var ans string
	err := row.Scan(&ans)
	if err == sql.ErrNoRows {
		return false, nil

	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (mt *MeasurementGroup) tableExists(tableName string) (bool, error) {
	sql0 := "SELECT table_name " +
		"FROM information_schema.tables " +
		"WHERE table_schema = ? " +
		"AND table_name = ? " +
		"LIMIT 1"
	row := mt.db.QueryRow(sql0, mt.dbName, tableName)
	var ans string
	err := row.Scan(&ans)
	if err == sql.ErrNoRows {
		return false, nil

	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (mt *MeasurementGroup) WriteSync(p *Point) error {
	if p.Time().IsZero() {
		p.SetTime(time.Now().In(mt.location))
	}
	errTpl := "failed to write dataset point: %w"
	tx, err := mt.db.Begin()
	if err != nil {
		return fmt.Errorf(errTpl, err)
	}
	ins, err := tx.Exec(
		fmt.Sprintf("INSERT INTO %s (dt, name, value) VALUES (?, ?, ?)", mt.mkTable0Name()),
		p.Time(), p.Name(), p.Value(),
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf(errTpl, err)
	}
	if p.HasTags() {
		newID, err := ins.LastInsertId()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf(errTpl, err)
		}
		for k, v := range p.Tags() {
			_, err := tx.Exec(
				fmt.Sprintf("INSERT INTO %s (point_id, name, value) VALUES (?, ?, ?)", mt.mkTable1Name()),
				newID, k, v,
			)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf(errTpl, err)
	}
	return nil
}

func NewMeasurementGroup(
	conf *db.DBConf,
	location *time.Location,
	name string,
) (*MeasurementGroup, error) {
	db0, err := db.Open(conf)
	if err != nil {
		return nil, err
	}
	ans := &MeasurementGroup{
		db:       db0,
		dbName:   conf.Name,
		location: location,
		name:     name,
	}
	if err := ans.CreateTablesIfNone(); err != nil {
		return nil, err
	}
	return ans, nil
}
