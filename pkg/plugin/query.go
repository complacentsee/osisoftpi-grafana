package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

type Query struct {
	RefID         string `json:"RefID"`
	QueryType     string `json:"QueryType"`
	MaxDataPoints int    `json:"MaxDataPoints"`
	Interval      int64  `json:"Interval"`
	TimeRange     struct {
		From time.Time `json:"From"`
		To   time.Time `json:"To"`
	} `json:"TimeRange"`
	Pi PIWebAPIQuery `json:"JSON"`
}

func (q *Query) getIntervalTime() int {
	intervalTime := q.Pi.IntervalMs
	if intervalTime == 0 {
		intervalTime = int(q.Interval)
	}
	return intervalTime
}

func (q *Query) getTimeRangeURIComponent() string {
	return "?startTime=" + q.TimeRange.From.UTC().Format(time.RFC3339) + "&endTime=" + q.TimeRange.To.UTC().Format(time.RFC3339)
}

func (q *Query) streamingEnabled() bool {
	return q.Pi.EnableStreaming.Enable
}

func (q *Query) isStreamable() bool {
	return !q.Pi.isExpression() && q.streamingEnabled()
}

type PIWebAPIQuery struct {
	Attributes []struct {
		Label string `json:"label"`
		Value struct {
			Expandable bool   `json:"expandable"`
			Value      string `json:"value"`
		} `json:"value"`
	} `json:"attributes"`
	Datasource struct {
		Type string `json:"type"`
		UID  string `json:"uid"`
	} `json:"datasource"`
	DatasourceID  int `json:"datasourceId"`
	DigitalStates struct {
		Enable bool `json:"enable"`
	} `json:"digitalStates"`
	EnableStreaming struct {
		Enable bool `json:"enable"`
	} `json:"EnableStreaming"`
	ElementPath string `json:"elementPath"`
	Expression  string `json:"expression"`
	Hide        bool   `json:"hide"`
	Interpolate struct {
		Enable bool `json:"enable"`
	} `json:"interpolate"`
	IntervalMs     int  `json:"intervalMs"`
	IsPiPoint      bool `json:"isPiPoint"`
	MaxDataPoints  int  `json:"maxDataPoints"`
	RecordedValues struct {
		Enable    bool `json:"enable"`
		MaxNumber int  `json:"maxNumber"`
	} `json:"recordedValues"`
	RefID string `json:"refId"`
	Regex struct {
		Enable bool `json:"enable"`
	} `json:"regex"`
	Segments []struct {
		Label string `json:"label"`
		Value struct {
			Expandable bool   `json:"expandable"`
			Value      string `json:"value"`
			WebID      string `json:"webId"`
		} `json:"value"`
	} `json:"segments"`
	Summary QuerySummary `json:"summary"`
	Target  string       `json:"target"`
}

type QuerySummary struct {
	Basis    string        `json:"basis"`
	Interval string        `json:"interval"`
	Nodata   string        `json:"nodata"`
	Types    []SummaryType `json:"types"`
}

type SummaryType struct {
	Label string           `json:"label"`
	Value SummaryTYpeValue `json:"value"`
}

type SummaryTYpeValue struct {
	Expandable bool   `json:"expandable"`
	Value      string `json:"value"`
}

type PiProcessedQuery struct {
	Label               string `json:"Label"`
	WebID               string `json:"WebID"`
	UID                 string `json:"-"`
	IntervalNanoSeconds int64  `json:"IntervalNanoSeconds"`
	IsPIPoint           bool   `json:"IsPiPoint"`
	Streamable          bool   `json:"isStreamable"`
	FullTargetPath      string `json:"FullTargetPath"`
	ResponseUnits       string
	BatchRequest        BatchSubRequest `json:"BatchRequest"`
	Response            PiBatchData     `json:"ResponseData"`
}

type BatchSubRequest struct {
	Method   string `json:"Method"`
	Resource string `json:"Resource"`
}

func (d Datasource) processBatchtoFrames(processedQuery map[string][]PiProcessedQuery) *backend.QueryDataResponse {
	response := backend.NewQueryDataResponse()

	for RefID, query := range processedQuery {

		var subResponse backend.DataResponse
		for i, q := range query {
			backend.Logger.Info("Processing query", "RefID", RefID, "QueryIndex", i)
			Type := d.getTypeForWebID(q.WebID)
			DigitalState := d.getDigitalStateforWebID(q.WebID)
			frame, err := convertItemsToDataFrame(q.Label, *q.Response.getItems(), Type, DigitalState, false)
			frame.RefID = RefID
			frame.Meta.ExecutedQueryString = q.BatchRequest.Resource

			if err != nil {
				backend.Logger.Error("Error processing query", "RefID", RefID, "QueryIndex", i)
				subResponse.Frames = append(subResponse.Frames, frame)
				continue
			}

			// If the query is streamable, then we need to set the channel URI
			// and the executed query string.
			if q.Streamable {
				// Create a new channel for this frame request.
				// Creating a new channel for each frame request is not ideal,
				// but it is the only way to ensure that the frame data is refreshed
				// on a time interval update.
				channeluuid := uuid.New()
				channelURI := "ds/" + q.UID + "/" + channeluuid.String()
				channel := StreamChannelConstruct{
					WebID:               q.WebID,
					IntervalNanoSeconds: q.IntervalNanoSeconds,
				}
				d.channelConstruct[channeluuid.String()] = channel
				frame.Meta.Channel = channelURI
			}

			subResponse.Frames = append(subResponse.Frames, frame)
		}
		response.Responses[RefID] = subResponse
	}
	return response
}

func (d Datasource) batchRequest(ctx context.Context, processedQuery map[string][]PiProcessedQuery) map[string][]PiProcessedQuery {
	for RefID, processed := range processedQuery {
		batchRequest := make(map[string]BatchSubRequest)
		fmt.Println("RefID:", RefID)

		for i, p := range processed {
			batchRequest[fmt.Sprint(i)] = p.BatchRequest
		}
		r, err := d.apiBatchRequest(ctx, batchRequest)
		if err != nil {
			log.DefaultLogger.Error("Error in batch request", "error", err)
			continue
		}

		tempresponse := make(map[int]PIBatchResponse)
		err = json.Unmarshal(r, &tempresponse)
		if err != nil {
			log.DefaultLogger.Error("Error unmarshaling batch response", "error", err)
			continue
		}

		for i := range processed {
			processedQuery[RefID][i].Response = tempresponse[i].Content
		}
	}
	return processedQuery
}

func (q *PIWebAPIQuery) isSummary() bool {
	return q.Summary.Basis != "" && len(q.Summary.Types) > 0
}

func (q *PIWebAPIQuery) getSummaryDuration() string {
	if q.Summary.Interval == "" {
		return "30s"
	}
	return q.Summary.Interval
}

func (q *PIWebAPIQuery) getSummaryURIComponent() string {
	uri := ""
	// FIXME: Validate that we cannot have a summary for a calculation
	if !q.isExpression() {
		for _, t := range q.Summary.Types {
			uri += "&summaryType=" + t.Value.Value
		}
		uri += "&summaryBasis=" + q.Summary.Basis
		uri += "&summaryDuration=" + q.getSummaryDuration()
	}
	return uri
}

func (q *PIWebAPIQuery) isRecordedValues() bool {
	return q.RecordedValues.Enable
}

func (q *PIWebAPIQuery) isInterpolated() bool {
	return q.Interpolate.Enable
}

func (q *PIWebAPIQuery) isRegex() bool {
	return q.Regex.Enable
}

func (q *PIWebAPIQuery) isExpression() bool {
	return q.Expression != ""
}

func (q *PIWebAPIQuery) getBasePath() string {
	semiIndex := strings.Index(q.Target, ";")
	return q.Target[:semiIndex]
}

func (q *PIWebAPIQuery) getTargets() []string {
	semiIndex := strings.Index(q.Target, ";")
	return strings.Split(q.Target[semiIndex+1:], ";")
}

func (q *Query) getMaxDataPoints() int {
	maxDataPoints := q.Pi.MaxDataPoints
	if maxDataPoints == 0 {
		maxDataPoints = q.MaxDataPoints
	}
	return maxDataPoints
}

func (q Query) getQueryBaseURL() string {
	// TODO: validate all of the options.
	// Clean up this mess
	// Valid list:
	// - plot
	// - calulcation w/ interval
	// - recorded with no default max count override
	// - recorded with override max count
	// FIXME: Missing functionality
	//    - summary
	//    - regex replacement
	//    - display name updates
	//    - replace bad data

	var uri string
	if q.Pi.isExpression() {
		uri += "/calculation"
		if q.Pi.isSummary() {
			uri += "/summary" + q.getTimeRangeURIComponent()
			if q.Pi.isInterpolated() {
				uri += fmt.Sprintf("&sampleType=Interval&sampleInterval=%dms", q.getIntervalTime())
			}
		} else {
			uri += "/intervals" + q.getTimeRangeURIComponent()
			uri += fmt.Sprintf("&sampleInterval=%dms", q.getIntervalTime())
		}
		uri += "&expression=" + url.QueryEscape(q.Pi.Expression)
	} else {
		uri += "/streamsets"
		if q.Pi.isSummary() {
			uri += "/summary" + q.getTimeRangeURIComponent() + fmt.Sprintf("&intervals=%d", q.getMaxDataPoints())
			uri += q.Pi.getSummaryURIComponent()
		} else if q.Pi.isInterpolated() {
			uri += "/interpolated" + q.getTimeRangeURIComponent() + fmt.Sprintf("&interval=%d", q.getIntervalTime())
		} else if q.Pi.isRecordedValues() {
			uri += "/recorded" + q.getTimeRangeURIComponent() + fmt.Sprintf("&maxCount=%d", q.getMaxDataPoints())
		} else {
			uri += "/plot" + q.getTimeRangeURIComponent() + fmt.Sprintf("&intervals=%d", q.getMaxDataPoints())
		}
	}
	return uri
}

func (d Datasource) processQuery(ctx context.Context, query backend.DataQuery, datasourceUID string) []PiProcessedQuery {
	var response []PiProcessedQuery
	var PiQuery Query
	tempJson, err := json.Marshal(query)
	if err != nil {
		log.DefaultLogger.Error("Error marshalling query", "error", err)
		return response
	}

	err1 := json.Unmarshal(tempJson, &PiQuery)
	if err1 != nil {
		log.DefaultLogger.Error("Error unmarshalling query", "error", err)
		return response
	}

	for _, target := range PiQuery.Pi.getTargets() {
		fullTargetPath := PiQuery.Pi.getBasePath()
		if PiQuery.Pi.IsPiPoint {
			fullTargetPath += "\\" + target
		} else {
			fullTargetPath += "|" + target
		}
		WebID, err := d.getWebID(ctx, fullTargetPath, PiQuery.Pi.IsPiPoint)
		if err != nil {
			log.DefaultLogger.Error("Error getting WebID", "error", err)
			continue
		}
		batchSubRequest := BatchSubRequest{
			Method:   "GET",
			Resource: d.settings.URL + PiQuery.getQueryBaseURL() + "&webid=" + WebID.WebID,
		}

		piQuery := PiProcessedQuery{
			Label:               target,
			UID:                 datasourceUID,
			WebID:               WebID.WebID,
			IntervalNanoSeconds: PiQuery.Interval,
			IsPIPoint:           PiQuery.Pi.IsPiPoint,
			Streamable:          PiQuery.isStreamable(),
			FullTargetPath:      fullTargetPath,
			BatchRequest:        batchSubRequest,
		}
		response = append(response, piQuery)
	}
	return response
}

type ErrorResponse struct {
	Errors []string `json:"Errors"`
}

type PIBatchResponse struct {
	Status  int               `json:"Status"`
	Headers map[string]string `json:"Headers"`
	Content PiBatchData       `json:"Content"`
}

type PIBatchResponseBase struct {
	Status  int               `json:"Status"`
	Headers map[string]string `json:"Headers"`
}

type PiBatchData interface {
	getUnits() string
	getItems() *[]PiBatchContentItem
}

type PiBatchDataError struct {
	Error *ErrorResponse
}

func (p PiBatchDataError) getUnits() string {
	return ""
}

func (p PiBatchDataError) getItems() *[]PiBatchContentItem {
	var items []PiBatchContentItem
	return &items
}

// func (p PiBatchDataError) getDataFrame(frameName string, isStreamable bool) (*data.Frame, error) {
// 	frame := data.NewFrame(frameName)
// 	frame.AppendNotices(data.Notice{
// 		Severity: data.NoticeSeverityError,
// 		Text:     p.Error.Errors[0],
// 	})
// 	return frame, fmt.Errorf(p.Error.Errors[0])
// }

type PiBatchDataWithSubItems struct {
	Links map[string]interface{} `json:"Links"`
	Items []struct {
		WebId             string               `json:"WebId"`
		Name              string               `json:"Name"`
		Path              string               `json:"Path"`
		Links             PiBatchContentLinks  `json:"Links"`
		Items             []PiBatchContentItem `json:"Items"`
		UnitsAbbreviation string               `json:"UnitsAbbreviation"`
	} `json:"Items"`
	Error *string
}

func (p PiBatchDataWithSubItems) getUnits() string {
	return p.Items[0].UnitsAbbreviation
}

func (p PiBatchDataWithSubItems) getItems() *[]PiBatchContentItem {
	return &p.Items[0].Items
}

type PiBatchDataWithoutSubItems struct {
	Links             map[string]interface{} `json:"Links"`
	Items             []PiBatchContentItem   `json:"Items"`
	UnitsAbbreviation string                 `json:"UnitsAbbreviation"`
}

func (p PiBatchDataWithoutSubItems) getUnits() string {
	return p.UnitsAbbreviation
}

func (p PiBatchDataWithoutSubItems) getItems() *[]PiBatchContentItem {
	return &p.Items
}

// Custom unmarshaler to unmarshal  PIBatchResponse to the correct struct type.
// If the first item in the Items array has a WebId, then we have a PiBatchDataWithSubItems
// If the first item in the Items array does not have a WebId, then we have a PiBatchDataWithoutSubItems
// All other formations will return an PiBatchDataError
func (p *PIBatchResponse) UnmarshalJSON(data []byte) error {
	var PIBatchResponseBase PIBatchResponseBase
	json.Unmarshal(data, &PIBatchResponseBase)
	p.Status = PIBatchResponseBase.Status
	p.Headers = PIBatchResponseBase.Headers

	// // Unmarshal into a generic map to get the "Items" key
	// // Determine if Items[0].WebId is valid. If it is,
	// // then we have a PiBatchDataWithSubItems
	var rawData map[string]interface{}
	err := json.Unmarshal(data, &rawData)
	if err != nil {
		backend.Logger.Info("Error unmarshalling batch response", err)
		return err
	}

	Content, ok := rawData["Content"].(map[string]interface{})
	if !ok {
		backend.Logger.Error("key 'Content' not found in raw JSON", "rawData", rawData)
		return fmt.Errorf("key 'Content' not found in raw JSON")
	}

	rawContent, _ := json.Marshal(Content)

	if p.Status != http.StatusOK {
		temp_error := &ErrorResponse{}
		err = json.Unmarshal(rawContent, temp_error)
		if err != nil {
			backend.Logger.Error("Error Batch Error Response", "Error", err)
			return err
		}
		p.Content = createPiBatchDataError(&temp_error.Errors)
		return nil
	}

	items, ok := Content["Items"].([]interface{})
	if !ok {
		backend.Logger.Error("key 'Items' not found in 'Content'", "Content", Content)
		//Return an error Batch Data Response to the user is notified
		errMessages := &[]string{"Could not process response from PI Web API"}
		p.Content = createPiBatchDataError(errMessages)
		return nil
	}

	item, ok := items[0].(map[string]interface{})
	if !ok {
		backend.Logger.Error("key '0' not found in 'Items'", "Items", items)
		//Return an error Batch Data Response to the user is notified
		errMessages := &[]string{"Could not process response from PI Web API"}
		p.Content = createPiBatchDataError(errMessages)
		return nil
	}

	// Check if the response contained a WebId, if the response did contain a WebID
	// then it is a PiBatchDataWithSubItems, otherwise it is a PiBatchDataWithoutSubItems
	_, ok = item["WebId"].(string)

	if !ok {
		ResContent := PiBatchDataWithoutSubItems{}
		err = json.Unmarshal(rawContent, &ResContent)
		if err != nil {
			backend.Logger.Info("Error unmarshalling batch response", err)
			//Return an error Batch Data Response so the user is notified
			errMessages := &[]string{"Could not process response from PI Web API"}
			p.Content = createPiBatchDataError(errMessages)
			return nil
		}
		p.Content = ResContent
		return nil
	}
	ResContent := PiBatchDataWithSubItems{}
	err = json.Unmarshal(rawContent, &ResContent)
	if err != nil {
		backend.Logger.Info("Error unmarshalling batch response", err)
		//Return an error Batch Data Response to the user is notified
		errMessages := &[]string{"Could not process response from PI Web API"}
		p.Content = createPiBatchDataError(errMessages)
		return nil
	}
	p.Content = ResContent
	return nil
}

func createPiBatchDataError(errorMessage *[]string) *PiBatchDataError {
	errorResponse := &ErrorResponse{Errors: *errorMessage}
	resContent := &PiBatchDataError{Error: errorResponse}
	return resContent
}

type PiBatchContentLinks struct {
	Source string `json:"Source"`
}

type PiBatchContentItems struct {
	WebId             string               `json:"WebId"`
	Name              string               `json:"Name"`
	Path              string               `json:"Path"`
	Links             PiBatchContentLinks  `json:"Links"`
	Items             []PiBatchContentItem `json:"Items"`
	UnitsAbbreviation string               `json:"UnitsAbbreviation"`
}

type PiBatchContentItem struct {
	Timestamp         string      `json:"Timestamp"`
	Value             interface{} `json:"Value"`
	UnitsAbbreviation string      `json:"UnitsAbbreviation"`
	Good              bool        `json:"Good"`
	Questionable      bool        `json:"Questionable"`
	Substituted       bool        `json:"Substituted"`
	Annotated         bool        `json:"Annotated"`
}
