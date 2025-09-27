package reconstruct_loki

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"unsafe"
	"github.com/brody192/locomotive/internal/logline/reconstructor"
	"github.com/brody192/locomotive/internal/railway/subscribe/environment_logs"
	"github.com/tidwall/sjson"
)

func EnvironmentLogStreams(logs []environment_logs.EnvironmentLogWithMetadata) ([]byte, error) {
	streams := lokiJSON
	for i := range logs {
		// DEBUG: Log what Railway is actually providing
		log.Printf("=== DEBUG LOG %d ===", i)
		log.Printf("Message: '%s'", logs[i].Log.Message)
		log.Printf("Severity: '%s'", logs[i].Log.Severity)
		log.Printf("Attributes count: %d", len(logs[i].Log.Attributes))
		for j, attr := range logs[i].Log.Attributes {
			log.Printf("  Attr[%d]: Key='%s' | Value='%s'", j, attr.Key, attr.Value)
		}
		log.Printf("=== END DEBUG ===")

		// Set stream labels from metadata
		for key, value := range logs[i].Metadata {
			streams, _ = sjson.Set(streams, fmt.Sprintf("streams.%d.stream.%s", i, key), value)
		}
		
		// Set timestamp
		timestamp := strconv.FormatInt(cmp.Or(reconstructor.TryExtractTimestamp(logs[i]), logs[i].Log.Timestamp).UnixNano(), 10)
		streams, _ = sjson.Set(streams, fmt.Sprintf("streams.%d.values.0.0", i), timestamp)
		
		// Create JSON from available data
		jsonData := make(map[string]interface{})
		jsonData["msg"] = logs[i].Log.Message
		
		// Add all flattened attributes
		for j := range logs[i].Log.Attributes {
			for key, value := range jsonToAttributes(logs[i].Log.Attributes[j].Key, logs[i].Log.Attributes[j].Value) {
				jsonData[key] = value
			}
		}
		
		// Convert to JSON string
		jsonBytes, _ := json.Marshal(jsonData)
		streams, _ = sjson.Set(streams, fmt.Sprintf("streams.%d.values.0.1", i), string(jsonBytes))
	}
	return unsafe.Slice(unsafe.StringData(streams), len(streams)), nil
}
