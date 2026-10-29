package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Config struct {
	DataDirectory string            `json:"dataDirectory"`
	Theme         string            `json:"theme"`
	Display       DisplayConfig     `json:"display"`
	Concurrency   ConcurrencyConfig `json:"concurrency"`
	Cache         CacheConfig       `json:"cache"`
	Runtime       RuntimeConfig     `json:"runtime"`
	Availability  Availability      `json:"availability"`
	CommonCrawl   CommonCrawl       `json:"commonCrawl"`
}

type DisplayConfig struct {
	TUIEnabledByDefault  bool   `json:"tuiEnabledByDefault"`
	DefaultOutputFormat  string `json:"defaultOutputFormat"`
	ProgressIntervalSecs int    `json:"progressIntervalSeconds"`
	LogLevel             string `json:"logLevel"`
}

type ConcurrencyConfig struct {
	CDX       int `json:"cdx"`
	WARC      int `json:"warc"`
	WAT       int `json:"wat"`
	Graph     int `json:"graph"`
	DNS       int `json:"dns"`
	RDAP      int `json:"rdap"`
	Registrar int `json:"registrar"`
}

type CacheConfig struct {
	RegisteredTTLDays     int `json:"registeredTtlDays"`
	AvailableTTLHours     int `json:"availableTtlHours"`
	LikelyAvailableTTLHrs int `json:"likelyAvailableTtlHours"`
	AmbiguousTTLHours     int `json:"ambiguousTtlHours"`
	UnsupportedTLDTtlDays int `json:"unsupportedTldTtlDays"`
}

type RuntimeConfig struct {
	RequestTimeoutSeconds     int `json:"requestTimeoutSeconds"`
	MaxRetryAttempts          int `json:"maxRetryAttempts"`
	InitialRetryBackoffMillis int `json:"initialRetryBackoffMillis"`
	MaxRetryBackoffSeconds    int `json:"maxRetryBackoffSeconds"`
	QueueSize                 int `json:"queueSize"`
	SQLiteBatchRows           int `json:"sqliteBatchRows"`
	SQLiteBatchMaxDelayMillis int `json:"sqliteBatchMaxDelayMillis"`
}

type Availability struct {
	DNSResolvers           []string `json:"dnsResolvers"`
	RegistrarProvider      *string  `json:"registrarProvider"`
	RegistrarAPIKeyRef     *string  `json:"registrarApiKeyRef"`
	RegistrarSecretRef     *string  `json:"registrarSecretKeyRef"`
	RegistrarCheckStatuses []string `json:"registrarCheckStatuses"`
	CheckAllEligibleTLDs   bool     `json:"checkAllEligibleTlds"`
	BlockedTLDs            []string `json:"blockedTlds"`
	BlockedSuffixes        []string `json:"blockedSuffixes"`
	BlockedDomains         []string `json:"blockedDomains"`
	RestrictedTLDs         []string `json:"restrictedTlds"`
}

type CommonCrawl struct {
	IndexBaseURL        string         `json:"indexBaseUrl"`
	DataBaseURL         string         `json:"dataBaseUrl"`
	UserAgent           string         `json:"userAgent"`
	CDXPageSize         int            `json:"cdxPageSize"`
	MaxWARCRecordBytes  int64          `json:"maxWarcRecordBytes"`
	MaxWARCDecodedBytes int64          `json:"maxWarcDecodedBytes"`
	MaxWATObjectBytes   int64          `json:"maxWatObjectBytes"`
	MaxGraphObjectBytes int64          `json:"maxGraphObjectBytes"`
	WATBatchRows        int            `json:"watBatchRows"`
	GraphReleases       []GraphRelease `json:"graphReleases,omitempty"`
}

type GraphRelease struct {
	ID         string   `json:"id"`
	Months     []string `json:"months,omitempty"`
	CrawlIDs   []string `json:"crawlIds,omitempty"`
	VertexPath string   `json:"vertexPath"`
	EdgePath   string   `json:"edgePath"`
}

func Default() Config {
	provider := "porkbun"
	return Config{
		DataDirectory: DefaultDataDir(),
		Theme:         "dark",
		Display: DisplayConfig{
			TUIEnabledByDefault:  true,
			DefaultOutputFormat:  "text",
			ProgressIntervalSecs: 10,
			LogLevel:             "info",
		},
		Concurrency: ConcurrencyConfig{
			CDX:       4,
			WARC:      48,
			WAT:       8,
			Graph:     4,
			DNS:       300,
			RDAP:      20,
			Registrar: 10,
		},
		Cache: CacheConfig{
			RegisteredTTLDays:     14,
			AvailableTTLHours:     1,
			LikelyAvailableTTLHrs: 6,
			AmbiguousTTLHours:     12,
			UnsupportedTLDTtlDays: 60,
		},
		Runtime: RuntimeConfig{
			RequestTimeoutSeconds:     60,
			MaxRetryAttempts:          7,
			InitialRetryBackoffMillis: 500,
			MaxRetryBackoffSeconds:    60,
			QueueSize:                 10000,
			SQLiteBatchRows:           1000,
			SQLiteBatchMaxDelayMillis: 2000,
		},
		Availability: Availability{
			DNSResolvers:           []string{"1.1.1.1", "8.8.8.8"},
			RegistrarProvider:      &provider,
			RegistrarCheckStatuses: []string{"available_likely"},
			CheckAllEligibleTLDs:   true,
			RestrictedTLDs:         []string{"gov", "mil", "edu", "int", "arpa"},
		},
		CommonCrawl: CommonCrawl{
			IndexBaseURL:        "https://index.commoncrawl.org",
			DataBaseURL:         "https://data.commoncrawl.org",
			UserAgent:           "ExpiredDomainFinder/1.0",
			CDXPageSize:         1000,
			MaxWARCRecordBytes:  8 * 1024 * 1024,
			MaxWARCDecodedBytes: 16 * 1024 * 1024,
			MaxWATObjectBytes:   2 * 1024 * 1024 * 1024,
			MaxGraphObjectBytes: 2 * 1024 * 1024 * 1024,
			WATBatchRows:        1000,
		},
	}
}

func DefaultDataDir() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "expired-domain-finder")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".local", "share", "expired-domain-finder")
	}
	return ".expired-domain-finder"
}

func (c Config) Redacted() Config {
	c.Availability.RegistrarAPIKeyRef = nil
	c.Availability.RegistrarSecretRef = nil
	return c
}

func ConfigPath(dataDir string) string {
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}
	return filepath.Join(dataDir, "config.json")
}

func Load(path string) (Config, error) {
	if path == "" {
		path = ConfigPath(DefaultDataDir())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			cfg := Default()
			if dir := filepath.Dir(path); dir != "." {
				cfg.DataDirectory = dir
			}
			return cfg, nil
		}
		return Config{}, err
	}
	cfg := Default()
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.DataDirectory == "" {
		cfg.DataDirectory = filepath.Dir(path)
	}
	return cfg, cfg.Validate()
}

func Save(path string, cfg Config) error {
	if path == "" {
		path = ConfigPath(cfg.DataDirectory)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func (c Config) Validate() error {
	if c.DataDirectory == "" {
		return fmt.Errorf("data directory is required")
	}
	switch c.Display.DefaultOutputFormat {
	case "", "text", "json":
	default:
		return fmt.Errorf("unsupported output format: %s", c.Display.DefaultOutputFormat)
	}
	if c.Display.ProgressIntervalSecs != 10 {
		return fmt.Errorf("progress interval must be 10 seconds")
	}
	if c.Runtime.QueueSize <= 0 {
		return fmt.Errorf("queue size must be positive")
	}
	if c.Runtime.SQLiteBatchRows <= 0 {
		return fmt.Errorf("sqlite batch rows must be positive")
	}
	if c.CommonCrawl.CDXPageSize <= 0 {
		return fmt.Errorf("commonCrawl.cdxPageSize must be positive")
	}
	if c.CommonCrawl.MaxWARCRecordBytes <= 0 || c.CommonCrawl.MaxWARCDecodedBytes <= 0 || c.CommonCrawl.MaxWATObjectBytes <= 0 || c.CommonCrawl.MaxGraphObjectBytes <= 0 {
		return fmt.Errorf("common crawl byte limits must be positive")
	}
	if c.CommonCrawl.WATBatchRows <= 0 {
		return fmt.Errorf("commonCrawl.watBatchRows must be positive")
	}
	for name, value := range map[string]int{
		"cdx":       c.Concurrency.CDX,
		"warc":      c.Concurrency.WARC,
		"wat":       c.Concurrency.WAT,
		"graph":     c.Concurrency.Graph,
		"dns":       c.Concurrency.DNS,
		"rdap":      c.Concurrency.RDAP,
		"registrar": c.Concurrency.Registrar,
	} {
		if value <= 0 {
			return fmt.Errorf("%s concurrency must be positive", name)
		}
	}
	for _, release := range c.CommonCrawl.GraphReleases {
		if release.ID == "" {
			return fmt.Errorf("graph release id is required")
		}
		if release.VertexPath == "" || release.EdgePath == "" {
			return fmt.Errorf("graph release %s requires vertexPath and edgePath", release.ID)
		}
		if len(release.Months) == 0 && len(release.CrawlIDs) == 0 {
			return fmt.Errorf("graph release %s requires at least one month or crawlId", release.ID)
		}
	}
	return nil
}
