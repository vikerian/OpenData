// ============================================
// SHARED TYPES AND UTILITIES
// ============================================
// shared/types.go
package shared

import "time"

// DataPacket represents data fetched from open data sources
type DataPacket struct {
	ID        string                 `json:"id" bson:"_id"`
	Source    string                 `json:"source" bson:"source"`
	Dataset   string                 `json:"dataset" bson:"dataset"`
	Timestamp time.Time              `json:"timestamp" bson:"timestamp"`
	Format    string                 `json:"format" bson:"format"`
	Data      map[string]interface{} `json:"data" bson:"data"`
	Metadata  DataMetadata           `json:"metadata" bson:"metadata"`
}

// DataMetadata contains metadata about the dataset
type DataMetadata struct {
	Title       string   `json:"title" bson:"title"`
	Description string   `json:"description" bson:"description"`
	Tags        []string `json:"tags" bson:"tags"`
	Region      string   `json:"region" bson:"region"`
	Period      string   `json:"period" bson:"period"`
	Unit        string   `json:"unit" bson:"unit"`
}

// RegionData represents processed regional data
type RegionData struct {
	RegionCode string    `json:"region_code" bson:"region_code"`
	RegionName string    `json:"region_name" bson:"region_name"`
	Lat        float64   `json:"lat" bson:"lat"`
	Lon        float64   `json:"lon" bson:"lon"`
	Value      float64   `json:"value" bson:"value"`
	Timestamp  time.Time `json:"timestamp" bson:"timestamp"`
	Dataset    string    `json:"dataset" bson:"dataset"`
}

// QueryRequest represents a data query
type QueryRequest struct {
	Dataset   string    `json:"dataset"`
	StartDate time.Time `json:"start_date"`
	EndDate   time.Time `json:"end_date"`
	Regions   []string  `json:"regions"`
	Metric    string    `json:"metric"`
}

// DataFrame represents query results
type DataFrame struct {
	Query   QueryRequest `json:"query"`
	Data    []RegionData `json:"data"`
	Summary Statistics   `json:"summary"`
}

// Statistics contains summary statistics
type Statistics struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"std_dev"`
}

// Message types for ZeroMQ
const (
	MsgTypeData      = "DATA"
	MsgTypeHeartbeat = "HEARTBEAT"
	MsgTypeAck       = "ACK"
)

// ZMQMessage wraps messages for ZeroMQ transport
type ZMQMessage struct {
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Payload   []byte    `json:"payload"`
}
