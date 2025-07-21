// ============================================
// 1. DATA GATHERING MICROSERVICE
// ============================================
// services/gatherer/main.go
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/robfig/cron/v3"
)

// Config holds service configuration
type Config struct {
	PublisherAddr  string
	OpenDataAPIURL string
	CronSchedule   string
}

// DataGatherer handles data collection
type DataGatherer struct {
	config     Config
	publisher  *zmq4.Socket
	httpClient *http.Client
	cron       *cron.Cron
}

// NewDataGatherer creates a new gatherer instance
func NewDataGatherer(config Config) (*DataGatherer, error) {
	// Create ZMQ publisher socket
	publisher, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	if err := publisher.Bind(config.PublisherAddr); err != nil {
		return nil, fmt.Errorf("failed to bind publisher: %w", err)
	}

	return &DataGatherer{
		config:     config,
		publisher:  publisher,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cron:       cron.New(),
	}, nil
}

// Start begins the data gathering process
func (g *DataGatherer) Start(ctx context.Context) error {
	// Give subscribers time to connect
	time.Sleep(1 * time.Second)

	// Schedule data gathering
	_, err := g.cron.AddFunc(g.config.CronSchedule, func() {
		if err := g.gatherData(ctx); err != nil {
			log.Printf("Error gathering data: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("error scheduling cron job: %w", err)
	}

	g.cron.Start()

	// Initial data gathering
	if err := g.gatherData(ctx); err != nil {
		log.Printf("Initial data gathering error: %v", err)
	}

	// Heartbeat loop
	go g.heartbeatLoop(ctx)

	return nil
}

// heartbeatLoop sends periodic heartbeats
func (g *DataGatherer) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := ZMQMessage{
				Type:      MsgTypeHeartbeat,
				Timestamp: time.Now(),
			}
			data, _ := json.Marshal(msg)
			g.publisher.Send(string(data), 0)
		}
	}
}

// gatherData fetches data from Czech open data sources
func (g *DataGatherer) gatherData(ctx context.Context) error {
	datasets := []struct {
		ID     string
		Name   string
		URL    string
		Format string
	}{
		{
			ID:     "population-density",
			Name:   "Population Density by Region",
			URL:    "https://www.czso.cz/documents/10180/142756350/130055-21data043021.csv",
			Format: "csv",
		},
		{
			ID:     "unemployment",
			Name:   "Unemployment Rate by Region",
			URL:    "https://data.gov.cz/api/3/action/datastore_search?resource_id=unemployment-2024",
			Format: "json",
		},
		{
			ID:     "average-salary",
			Name:   "Average Salary by Region",
			URL:    "https://www.czso.cz/documents/10180/142756350/110080-21q2data.csv",
			Format: "csv",
		},
		// Add more datasets as needed
	}

	for _, dataset := range datasets {
		log.Printf("Fetching dataset: %s", dataset.Name)

		packet, err := g.fetchDataset(dataset.ID, dataset.Name, dataset.URL, dataset.Format)
		if err != nil {
			log.Printf("Error fetching dataset %s: %v", dataset.Name, err)
			continue
		}

		// Publish to ZeroMQ
		if err := g.publishPacket(ctx, packet); err != nil {
			log.Printf("Error publishing packet: %v", err)
		} else {
			log.Printf("Published dataset: %s", dataset.Name)
		}
	}

	return nil
}

// fetchDataset fetches a single dataset
func (g *DataGatherer) fetchDataset(id, name, url, format string) (*DataPacket, error) {
	resp, err := g.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error fetching data: %w", err)
	}
	defer resp.Body.Close()

	packet := &DataPacket{
		ID:        fmt.Sprintf("%s-%d", id, time.Now().Unix()),
		Source:    url,
		Dataset:   id,
		Timestamp: time.Now(),
		Format:    format,
		Metadata: DataMetadata{
			Title: name,
		},
	}

	switch format {
	case "csv":
		data, err := g.parseCSV(resp.Body)
		if err != nil {
			return nil, err
		}
		packet.Data = data
	case "json":
		var data map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return nil, err
		}
		packet.Data = data
	}

	return packet, nil
}

// parseCSV parses CSV data
func (g *DataGatherer) parseCSV(r io.Reader) (map[string]interface{}, error) {
	reader := csv.NewReader(r)
	reader.Comma = ';'
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("insufficient data")
	}

	headers := records[0]
	data := make(map[string]interface{})
	rows := make([]map[string]string, 0)

	for i := 1; i < len(records); i++ {
		row := make(map[string]string)
		for j, value := range records[i] {
			if j < len(headers) {
				row[headers[j]] = strings.TrimSpace(value)
			}
		}
		rows = append(rows, row)
	}

	data["headers"] = headers
	data["rows"] = rows
	return data, nil
}

// publishPacket publishes data packet via ZeroMQ
func (g *DataGatherer) publishPacket(ctx context.Context, packet *DataPacket) error {
	packetData, err := json.Marshal(packet)
	if err != nil {
		return fmt.Errorf("error marshaling packet: %w", err)
	}

	msg := ZMQMessage{
		Type:      MsgTypeData,
		Timestamp: time.Now(),
		Payload:   packetData,
	}

	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshaling message: %w", err)
	}

	// Send with topic prefix for filtering
	topic := fmt.Sprintf("%s.%s", packet.Dataset, MsgTypeData)
	_, err = g.publisher.Send(topic+" "+string(msgData), 0)

	return err
}

// Cleanup closes ZeroMQ socket
func (g *DataGatherer) Cleanup() {
	if g.publisher != nil {
		g.publisher.Close()
	}
}

// Gatherer main function
func main() {
	config := Config{
		PublisherAddr:  os.Getenv("ZMQ_PUBLISHER_ADDR"),
		OpenDataAPIURL: "https://data.gov.cz/api/3/action/",
		CronSchedule:   os.Getenv("CRON_SCHEDULE"),
	}

	if config.PublisherAddr == "" {
		config.PublisherAddr = "tcp://*:5555"
	}
	if config.CronSchedule == "" {
		config.CronSchedule = "0 */6 * * *" // Every 6 hours
	}

	gatherer, err := NewDataGatherer(config)
	if err != nil {
		log.Fatal(err)
	}
	defer gatherer.Cleanup()

	ctx := context.Background()

	log.Printf("Starting data gatherer service on %s", config.PublisherAddr)
	if err := gatherer.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Keep running
	select {}
}
