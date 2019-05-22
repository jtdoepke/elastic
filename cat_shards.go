package elastic

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/olivere/elastic/v7/uritemplates"
)

// CatShardsService returns the list of shards plus some additional
// information about them.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/cat-shards.html
// for details.
type CatShardsService struct {
	client        *Client
	pretty        bool
	index         string
	bytes         string // b, k, m, or g
	local         *bool
	masterTimeout string
	columns       []string
	sort          []string // list of columns for sort order
}

// NewCatShardsService creates a new CatShardsService.
func NewCatShardsService(client *Client) *CatShardsService {
	return &CatShardsService{
		client: client,
	}
}

// Limit response to shards of indices matching this pattern
// (by default shards from all indices are returned).
func (s *CatShardsService) Index(index string) *CatShardsService {
	s.index = index
	return s
}

// Bytes represents the unit in which to display byte values.
// Valid values are: "b", "k", "m", or "g".
func (s *CatShardsService) Bytes(bytes string) *CatShardsService {
	s.bytes = bytes
	return s
}

// Local indicates to return local information, i.e. do not retrieve
// the state from master node (default: false).
func (s *CatShardsService) Local(local bool) *CatShardsService {
	s.local = &local
	return s
}

// MasterTimeout is the explicit operation timeout for connection to master node.
func (s *CatShardsService) MasterTimeout(masterTimeout string) *CatShardsService {
	s.masterTimeout = masterTimeout
	return s
}

// Columns to return in the response.
// To get a list of all possible columns to return, run the following command
// in your terminal:
//
// Example:
//   curl 'http://localhost:9200/_cat/shards?help'
//
// You can use Columns("*") to return all possible columns. That might take
// a little longer than the default set of columns.
func (s *CatShardsService) Columns(columns ...string) *CatShardsService {
	s.columns = columns
	return s
}

// Sort is a list of fields to sort by.
func (s *CatShardsService) Sort(fields ...string) *CatShardsService {
	s.sort = fields
	return s
}

// Pretty indicates that the JSON response be indented and human readable.
func (s *CatShardsService) Pretty(pretty bool) *CatShardsService {
	s.pretty = pretty
	return s
}

// buildURL builds the URL for the operation.
func (s *CatShardsService) buildURL() (string, url.Values, error) {
	// Build URL
	var (
		path string
		err  error
	)

	if s.index != "" {
		path, err = uritemplates.Expand("/_cat/shards/{index}", map[string]string{
			"index": s.index,
		})
	} else {
		path = "/_cat/shards"
	}
	if err != nil {
		return "", url.Values{}, err
	}

	// Add query string parameters
	params := url.Values{
		"format": []string{"json"}, // always returns as JSON
	}
	if s.pretty {
		params.Set("pretty", "true")
	}
	if s.bytes != "" {
		params.Set("bytes", s.bytes)
	}
	if v := s.local; v != nil {
		params.Set("local", fmt.Sprint(*v))
	}
	if s.masterTimeout != "" {
		params.Set("master_timeout", s.masterTimeout)
	}
	if len(s.columns) > 0 {
		params.Set("h", strings.Join(s.columns, ","))
	}
	if len(s.sort) > 0 {
		params.Set("s", strings.Join(s.sort, ","))
	}
	return path, params, nil
}

// Do executes the operation.
func (s *CatShardsService) Do(ctx context.Context) (CatShardsResponse, error) {
	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}

	// Get HTTP response
	res, err := s.client.PerformRequest(ctx, PerformRequestOptions{
		Method: "GET",
		Path:   path,
		Params: params,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	var ret CatShardsResponse
	if err := s.client.decoder.Decode(res.Body, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// -- Result of a get request.

// CatShardsResponse is the outcome of CatShardsService.Do.
type CatShardsResponse []CatShardsResponseRow

// CatShardsResponseRow specifies the data returned for one shard
// of a CatShardsResponse. Notice that not all of these fields might
// be filled; that depends on the number of columns chose in the
// request (see CatShardsService.Columns).
type CatShardsResponseRow struct {
	Index                          string     `json:"index"`                           // index name
	Shard                          string     `json:"shard"`                           // shard name
	PriRep                         string     `json:"prirep"`                          // primary or replica shard, e.g. "p", "r"
	State                          string     `json:"state"`                           // shard state, e.g.
	Docs                           int        `json:"docs,string"`                     // number of docs in shard
	Store                          string     `json:"store"`                           // store size of shard (how much disk it uses), e.g. "0b"
	IP                             string     `json:"ip"`                              // ip of node where it lives
	NodeID                         string     `json:"id"`                              // unique id of node where it lives
	NodeName                       string     `json:"node"`                            // name of node where it lives
	SyncID                         string     `json:"sync_id"`                         // sync id
	UnassignedReason               string     `json:"unassigned.reason"`               // reason shard is unassigned (https://www.elastic.co/guide/en/elasticsearch/reference/7.0/cat-shards.html#reason-unassigned)
	UnassignedAt                   *time.Time `json:"unassigned.at"`                   // time shard became unassigned (UTC)
	UnassignedFor                  string     `json:"unassigned.for"`                  // time has been unassigned, e.g. "0s"
	UnassignedDeatils              string     `json:"unassigned.details"`              // additional details as to why the shard became unassigned
	RecoverySourceType             string     `json:"recoverysource.type"`             // recovery source type
	CompletionSize                 string     `json:"completion.size"`                 // size of completion, e.g. "0b"
	FieldDataMemorySize            string     `json:"fielddata.memory_size"`           // used fielddata cache, e.g. "0b"
	FieldDataEvictions             int        `json:"fielddata.evictions,string"`      // fielddata evictions
	QueryCacheMemorySize           string     `json:"query_cache.memory_size"`         // used query cache, e.g. "0b"
	QueryCacheEvictions            int        `json:"query_cache.evictions,string"`    // query cache evictions
	FlushTotal                     int        `json:"flush.total,string"`              // number of flushes
	FlushTotalTime                 string     `json:"flush.total_time"`                // time spent in flush, e.g. "0s"
	GetCurrent                     int        `json:"get.current,string"`              // number of current get ops
	GetTime                        string     `json:"get.time"`                        // time spent in get, e.g. "0s"
	GetTotal                       int        `json:"get.total,string"`                // number of get ops
	GetExistsTime                  string     `json:"get.exists_time"`                 // time spent in successful gets, e.g. "0s"
	GetExistsTotal                 int        `json:"get.exists_total,string"`         // number of successful gets
	GetMissingTime                 string     `json:"get.missing_time"`                // time spent in failed gets, e.g. "0s"
	GetMissingTotal                int        `json:"get.missing_total,string"`        // number of failed gets
	IndexingDeleteCurrent          int        `json:"indexing.delete_current,string"`  // number of current deletions
	IndexingDeleteTime             string     `json:"indexing.delete_time"`            // time spent in deletions, e.g. "0s"
	IndexingDeleteTotal            int        `json:"indexing.delete_total,string"`    // number of delete ops
	IndexingIndexCurrent           int        `json:"indexing.index_current,string"`   // number of current indexing ops
	IndexingIndexTime              string     `json:"indexing.index_time"`             // time spent in indexing, e.g. "0s"
	IndexingIndexTotal             int        `json:"indexing.index_total,string"`     // number of indexing ops
	IndexingIndexFailed            int        `json:"indexing.index_failed,string"`    // number of failed indexing ops
	MergesCurrent                  int        `json:"merges.current,string"`           // number of current merges
	MergesCurrentDocs              int        `json:"merges.current_docs,string"`      // number of current merging docs
	MergesCurrentSize              string     `json:"merges.current_size"`             // size of current merges, e.g. "0b"
	MergesTotal                    int        `json:"merges.total,string"`             // number of completed merge ops
	MergesTotalDocs                int        `json:"merges.total_docs,string"`        // docs merged
	MergesTotalSize                string     `json:"merges.total_size"`               // size merged, e.g. "0b"
	MergesTotalTime                string     `json:"merges.total_time"`               // time spent in merges, e.g. "0s"
	RefreshTotal                   int        `json:"refresh.total,string"`            // total refreshes
	RefreshTime                    string     `json:"refresh.time"`                    // time spent in refreshes, e.g. "0s"
	RefreshListeners               int        `json:"refresh.listeners,string"`        // number of pending refresh listeners
	SearchFetchCurrent             int        `json:"search.fetch_current,string"`     // current fetch phase ops
	SearchFetchTime                string     `json:"search.fetch_time"`               // time spent in fetch phase, e.g. "0s"
	SearchFetchTotal               int        `json:"search.fetch_total,string"`       // total fetch ops
	SearchOpenContexts             int        `json:"search.open_contexts,string"`     // open search contexts
	SearchQueryCurrent             int        `json:"search.query_current,string"`     // current query phase ops
	SearchQueryTime                string     `json:"search.query_time"`               // time spent in query phase, e.g. "0s"
	SearchQueryTotal               int        `json:"search.query_total,string"`       // total query phase ops
	SearchScrollCurrent            int        `json:"search.scroll_current,string"`    // open scroll contexts
	SearchScrollTime               string     `json:"search.scroll_time"`              // time scroll contexts held open, e.g. "0s"
	SearchScrollTotal              int        `json:"search.scroll_total,string"`      // completed scroll contexts
	SegmentsCount                  int        `json:"segments.count,string"`           // number of segments
	SegmentsMemory                 string     `json:"segments.memory"`                 // memory used by segments, e.g. "0b"
	SegmentsIndexWriterMemory      string     `json:"segments.index_writer_memory"`    // memory used by index writer, e.g. "0b"
	SegmentsVersionMapMemory       string     `json:"segments.version_map_memory"`     // memory used by version map, e.g. "0b"
	SegmentsFixedBitsetMemory      string     `json:"segments.fixed_bitset_memory"`    // memory used by fixed bit sets for nested object field types and type filters for types referred in _parent fields, e.g. "0b"
	SequenceNumberMax              int        `json:"seq_no.max,string"`               // max sequence number
	SequenceNumberLocalCheckpoint  int        `json:"seq_no.local_checkpoint,string"`  // local checkpoint
	SequenceNumberGlobalCheckpoint int        `json:"seq_no.global_checkpoint,string"` // global checkpoint
	WarmerCurrent                  int        `json:"warmer.current,string"`           // current warmer ops
	WarmerTotal                    int        `json:"warmer.total,string"`             // total warmer ops
	WarmerTotalTime                string     `json:"warmer.total_time"`               // time spent in warmers
}
