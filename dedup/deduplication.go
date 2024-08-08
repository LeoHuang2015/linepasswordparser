package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "net/http/pprof" // 导入这个包来启用 pprof 端点
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/bits-and-blooms/bloom"
	"github.com/sirupsen/logrus"
)

// leohuang hivesec 20240123
/*
CREATE TABLE sgk.tmp_urluserpass
(
	`id` Int64,
    `username` String,
    `password` String,
    `url` String,
    `source` String,
    `sourcedate` Date,
    INDEX idx_username_password_url (username, password, url) TYPE minmax GRANULARITY 8192,
    INDEX idx_username (username) TYPE minmax GRANULARITY 8192,
    INDEX idx_url (url) TYPE minmax GRANULARITY 8192
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sourcedate)  -- 以月份为单位进行分区
PRIMARY KEY (id, username)
ORDER BY (id, username, sourcedate)
SETTINGS index_granularity = 8192;
*/

type DbDataRecord struct {
	ID         int64
	Username   string
	Password   string
	URL        string
	App        string
	Source     string
	SourceDate time.Time
}

type DbSourcedateRecord struct {
	SourceDate time.Time
	Count      int64
}

type DbMinMaxRecord struct {
	MinId int64
	MaxId int64
}

type QueryDataList []DbDataRecord
type RecordList []DbDataRecord
type ResultList []DbDataRecord

var (
	bloomFilter *bloom.BloomFilter
	readCT      int64
	logReadCT   int
	logger      *logrus.Logger

	// db          *sql.DB
)

func initEnv() {
	logger = logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	// logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetOutput(os.Stdout)
}


func getlastIDFromDB(client *sql.DB, table string) (int64, int64, error) {
	lastID := int64(0)
	rowsCount := int64(0)

	// Get the last ID
	err := client.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY id DESC LIMIT 1", table)).Scan(&lastID)
	if err != nil {
		return lastID, rowsCount, err
	}

	// Get the total rows count
	err = client.QueryRow(fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&rowsCount)
	if err != nil {
		return lastID, rowsCount, err
	}

	return lastID, rowsCount, nil
}

type insertFuncType func(client *sql.DB, resultQueue chan ResultList)

func insertToDBLinkcloud(client *sql.DB, resultQueue chan ResultList) {
	// oid := int64(0)
	nid := int64(0)

	for item := range resultQueue {
		if item == nil {
			break
		}
		// startTime := time.Now()

		tx, err := client.Begin()
		if err != nil {
			log.Printf("Error starting insertToDB: %v", err)
		}

		stmt, err := tx.Prepare("INSERT INTO tmp_urluserpass (id, username, password, url,source, sourcedate) VALUES (?, ?, ?, ?, ?, ?)")
		if err != nil {
			log.Printf("Error preparing  data: %v", err)
		}

		for _, row := range item {
			nid++
			_, err := stmt.Exec(nid, row.Username, row.Password, row.URL, row.Source, row.SourceDate)
			if err != nil {
				log.Printf("Error inserting data: %v", err)
				tx.Rollback()
			}
		}

		err = tx.Commit()
		if err != nil {
			log.Printf("Error commiting data: %v", err)
			tx.Rollback()
			stmt.Close()
		}

		stmt.Close()

		// log.Printf("[SaveData]%d, dataLen: %d, time: %v", len(item), len(resultQueue), time.Since(startTime))
	}

}

func processLines(lineQueue chan QueryDataList, resultQueue chan ResultList, bloomFilter *bloom.BloomFilter, deduplicationCt *int64) {
	var wg sync.WaitGroup
	for item := range lineQueue {
		if item == nil {
			break
		}
		// startTime := time.Now()
		saveData := ResultList{}
		tmp_dict := make(map[string]int64)

		for _, row := range item {
			info := fmt.Sprintf("%s_%s_%s", row.Username, row.Password, row.URL)
			if _, ok := tmp_dict[info]; !ok {
				tmp_dict[info] = row.ID
				if !bloomFilter.TestString(info) {
					saveData = append(saveData, row)
					wg.Add(1)
					go func(info string) {
						defer wg.Done()
						bloomFilter.AddString(info)
					}(info)
				} else {
					// log.Println("[Duplicate]", bloomFilter.TestString(info), " | ", row.Username, " | ", row.Password, " | ", row.URL, " | ", row.ID)
				}
			} else {
				// log.Println("[Duplicate Inner]", tmp_dict[info], "|", row.Username, " | ", row.Password, " | ", row.URL, " | ", row.ID)
			}

		}

		*deduplicationCt += int64(len(saveData))
		resultQueue <- saveData
		tmp_dict = nil
		saveData = nil
		wg.Wait()

		// log.Printf("[Process] %.2f%% = %d/%d , procLen: %d, dataLen: %d, time: %v", float64(len(saveData))/float64(len(item))*100, len(saveData), len(item), len(lineQueue), len(resultQueue), time.Since(startTime))
	}

	// process执行完成，通知插入数据库
	resultQueue <- nil
}

func main() {

	// go func() {
	// 	http.ListenAndServe("0.0.0.0:8899", nil)
	// }()

	initEnv()

	var partitionDataList []DbSourcedateRecord

	table := "urluserpass"

	readCT = int64(0)
	logReadCT = 0
	// var partitionCt int64
	deduplicationCt := int64(0)

	var wg sync.WaitGroup

	// 100w左右的数据量，后面db写入的时候是瓶颈； 20w以下，瓶颈在计算；50w db基本无压力；
	// ggbest ： 1*500w  , 0.00001)
	//  linkcloud： 1*300w  , 0.001)
	// 优化了多携程，降低内存压力.  瓶颈在clickhouse-server查询的内存压力，2*300w期间很容易打爆。 多并发容易内存打爆

	var chunkSize int64
	startTm := time.Now()

	log.Printf("Starting to load data from database. Table: %s", table)

	client, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?database=sgk")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	lastID, rowsCT, err := getlastIDFromDB(client, table)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize the Bloom filter。 fp太小会影响内存和处理性能，fp太大会导致误删除
	expectedInsertions := uint(rowsCT + 10)
	errRate := 0.001
	var insertFunc insertFuncType

	// 从数据库读取的数量量
	chunkSize = int64(5000000)

	// 每次处理的数据量
	processChunkSize := 2000000

	processCt := 0

	processQueue := make(chan QueryDataList, 3)
	insertdbQueue := make(chan ResultList, 2)

	insertFunc = insertToDBLinkcloud

	bloomFilter = bloom.NewWithEstimates(expectedInsertions, errRate)

	log.Printf("[Table Info] %s  [LastID] %d  [rows] %d [chunkSize] %d [processChunkSize] %d", table, lastID, rowsCT, chunkSize, processChunkSize)

	// Start the processing goroutine

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("[Process]goroutine start....")
		processLines(processQueue, insertdbQueue, bloomFilter, &deduplicationCt)
		log.Println("[Process Over]....")
	}()

	// Start the database insertion goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		// ggbest
		log.Println("[insertFunc]goroutine start....")
		// insertToDBGgbest(client, insertdbQueue)
		// insertToDBLinkcloud(client, insertdbQueue)
		insertFunc(client, insertdbQueue)
		log.Println("[Insert Over]....")
	}()

	log.Printf("[Begin]Start get query result....")

	// 查询分区总量
	partitionCountQuery := fmt.Sprintf("select sourcedate, count(*) from %s group by sourcedate", table)
	rows, err := client.Query(partitionCountQuery)
	if err != nil {
		log.Println("[Partition]Error executing query:", err)
	}

	for rows.Next() {
		partitionData := DbSourcedateRecord{}
		err := rows.Scan(&partitionData.SourceDate, &partitionData.Count)
		if err != nil {
			log.Println("Error retrieving rows:", err)
		}

		partitionDataList = append(partitionDataList, partitionData)
	}
	rows.Close()

	log.Printf("Partition count: %d....", len(partitionDataList))

	for _, sd := range partitionDataList {
		// debug
		// if sd.SourceDate.Format("2006-01-02") != "2024-01-01" {
		// 	continue
		// }

		// limit, offset方案不行，查询到后面会载入整个分区，内存会爆掉
		// 通过有索引的id,查询id的最小值和最大值
		mIdData := DbMinMaxRecord{}
		// 查询量很大，计算需要比较长时间
		mIdQuery := fmt.Sprintf("select min(id), max(id) from %s where sourcedate='%s'", table, sd.SourceDate.Format("2006-01-02"))
		rows, err := client.Query(mIdQuery)
		if err != nil {
			log.Fatalln("[Partition]Error executing query:", err)
		}
		for rows.Next() {
			err := rows.Scan(&mIdData.MinId, &mIdData.MaxId)
			if err != nil {
				log.Fatalln("Error retrieving mIdData rows:", err)
			}
		}
		rows.Close()

		log.Printf("Partition: %s rows: %d MinId:%d  MaxId: %d", sd.SourceDate.Format("2006-01-02"), sd.Count, mIdData.MinId, mIdData.MaxId)

		// 遍历分区
		startId := mIdData.MinId
		endId := mIdData.MinId + chunkSize
		for startId <= mIdData.MaxId {
			query := fmt.Sprintf("SELECT id,username,password,url,source,sourcedate FROM %s WHERE sourcedate='%v' and (id >= %d and id<%d)", table, sd.SourceDate.Format("2006-01-02"), startId, endId)

			// log.Println(query)
			// time.Sleep(time.Second * 3)

			// partitionStartTm := time.Now()
			var queryDataList QueryDataList
			processCt = 0

			// 执行查询
			rows, err := client.Query(query)
			if err != nil {
				log.Fatalln("[Data]Error executing query:", err)
			}

			// 处理查询结果
			for rows.Next() {
				dbdata := DbDataRecord{}
				err := rows.Scan(
					&dbdata.ID,
					&dbdata.Username,
					&dbdata.Password,
					&dbdata.URL,
					&dbdata.Source,
					&dbdata.SourceDate)
				if err != nil {
					log.Println("Error retrieving rows:", err)
				}
				processCt++
				queryDataList = append(queryDataList, dbdata)
				if processCt >= processChunkSize {
					readCT += int64(len(queryDataList))
					logReadCT += len(queryDataList)

					processQueue <- queryDataList
					queryDataList = nil
					processCt = 0
				}

			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error retrieving rows:", err)
			}
			rows.Close()

			readCT += int64(len(queryDataList))
			logReadCT += len(queryDataList)

			if len(queryDataList) > 0 {
				processQueue <- queryDataList
				queryDataList = nil
			}

			// log.Printf("[Read]tm: %v chunkSize: %d  startId: %d  endId: %d  processLen: %d", time.Since(partitionStartTm), chunkSize, startId, endId, len(processQueue))

			startId = endId
			endId += chunkSize

			if logReadCT > 10000000 {
				elapsed1 := time.Since(startTm).Minutes()
				taken_task := float64(readCT) / float64(rowsCT) * 100
				pretime_total := (elapsed1) / (float64(readCT) / float64(rowsCT))
				pretime_remaining := pretime_total - elapsed1
				log.Printf("[GetData] [%.2f%%] %d [elapsed]:%.2fm |[total]:%.2fm |[remaining]:%.2fm  [pending]|process: %d |insert db: %d", taken_task, readCT, elapsed1, pretime_total, pretime_remaining, len(processQueue), len(insertdbQueue))

				logReadCT = 0
			}

		}
	}

	log.Println("[Read Over]....")

	processQueue <- nil

	log.Println("Waiting....")
	wg.Wait()
	// Signal the processing goroutine to stop

	log.Println(len(processQueue), len(insertdbQueue))
	// Signal the processing and database insertion goroutines to stop
	close(processQueue)
	close(insertdbQueue)

	// Print the deduplication rate and elapsed time
	elapsedHours := time.Since(startTm).Hours()
	log.Printf("[Deduplication Over] [Total Rows] %d [ct] %d [De Rate] %.2f%% = %d / %d...[Elapsed] %.2fh",
		rowsCT,
		readCT,
		float64(deduplicationCt)/float64(readCT)*100,
		deduplicationCt,
		rowsCT,
		elapsedHours)

	// save bloomfter to file
	// 打开一个文件用于保存 Bloom Filter
	// log.Println("save bloomfilter to local file....")
	// file, err := os.Create("bloomfilter.gob")
	// if err != nil {
	// 	panic(err)
	// }
	// defer file.Close()

	// 创建一个编码器
	// enc := gob.NewEncoder(file)

	// 使用编码器将 Bloom Filter 序列化保存到文件
	// err = enc.Encode(bloomFilter)
	// if err != nil {
	// 	panic(err)
	// }

}
