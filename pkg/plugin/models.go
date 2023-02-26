package plugin

import (
	"net/http"
	"sync"

	"github.com/go-co-op/gocron"
	"github.com/gorilla/websocket"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	settings                  backend.DataSourceInstanceSettings
	StreamHandler             backend.StreamHandler
	httpClient                *http.Client
	webIDCache                map[string]WebIDCacheEntry
	channelConstruct          map[string]StreamChannelConstruct
	scheduler                 *gocron.Scheduler
	websocketConnectionsMutex *sync.Mutex
	sendersByWebIDMutex       *sync.Mutex
	websocketConnections      map[string]*websocket.Conn
	sendersByWebID            map[string]map[*backend.StreamSender]bool
	streamChannels            map[string]chan []byte
}
