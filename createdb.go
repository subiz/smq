package smq

/*

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/subiz/cassandra"
)
	err = cql.CreateTable(tblHubs, `
		queue ASCII,
		partition INT,
		group BIGINT,
		offset BIGINT,
		data BLOB,
		PRIMARY KEY ((queue, partition, group), offset)
	`)
	if err != nil {
		panic(err)
	}

	err = cql.CreateTable(tbIndices, `
		queue ASCII,
		partition INT,
		consumer_offset BIGINT,
		producer_offset BIGINT,
		PRIMARY KEY ((queue, partition))
	`)
	if err != nil {
		panic(err)
	}
}
*/
