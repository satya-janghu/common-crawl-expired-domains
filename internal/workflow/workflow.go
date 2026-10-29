package workflow

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"expired-domain-finder/internal/artifact"
	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/graph"
	"expired-domain-finder/internal/storage"
)

type Runner struct {
	Store       storage.Store
	Config      config.Config
	CommonCrawl commoncrawl.Client
	RDAP        availability.RDAPClient
	Registrar   availability.RegistrarProvider
	DNSResolver availability.DNSResolver
	WorkerID    string
}

const (
	cdxAPITimeout      = 45 * time.Second
	rawCDXIndexTimeout = 2 * time.Minute
)

func (r Runner) DiscoverCDX(ctx context.Context, runID string, sources, crawls []string, limit int) error {
	selections := make([]commoncrawl.CrawlSelection, 0, len(crawls))
	for _, crawl := range crawls {
		selections = append(selections, commoncrawl.CrawlSelection{Input: crawl, ResolvedID: crawl})
	}
	catalog, err := r.CommonCrawl.LoadCatalog(ctx)
	if err != nil {
		if containsMonthSelection(crawls) {
			return fmt.Errorf("resolve crawl months: load Common Crawl catalog: %w", err)
		}
	} else if len(catalog) > 0 {
		resolved, err := commoncrawl.ResolveSelectionsFromCatalog(crawls, catalog)
		if err != nil {
			return err
		}
		selections = resolved
		for _, selection := range selections {
			metadata := graphReleaseMetadata(catalog, selection.ResolvedID)
			_ = r.Store.UpdateCrawlResolution(ctx, runID, selection.Input, selection.ResolvedID, metadata)
		}
	}
	type job struct {
		source    string
		selection commoncrawl.CrawlSelection
	}
	var jobsList []job
	for _, source := range sources {
		for _, selection := range selections {
			jobsList = append(jobsList, job{source: source, selection: selection})
		}
	}
	if len(jobsList) == 0 {
		return nil
	}
	concurrency := r.Config.Concurrency.CDX
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(jobsList) {
		concurrency = len(jobsList)
	}
	jobs := make(chan job)
	errs := make(chan error, len(jobsList))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
					errs <- err
					continue
				}
				if err := r.discoverCDXSourceCrawl(ctx, runID, item.source, item.selection.ResolvedID, limit); err != nil {
					errs <- err
				}
			}
		}()
	}
enqueue:
	for _, item := range jobsList {
		select {
		case <-ctx.Done():
			break enqueue
		case jobs <- item:
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r Runner) discoverCDXSourceCrawl(ctx context.Context, runID, source, crawlID string, limit int) error {
	pageSize := r.Config.CommonCrawl.CDXPageSize
	if pageSize <= 0 {
		pageSize = 1000
	}
	total := 0
	for _, pattern := range []string{source + "/*", "*." + source + "/*"} {
		pageCtx, cancelPage := context.WithTimeout(ctx, cdxAPITimeout)
		pageCount, err := r.CommonCrawl.CDXPageCount(pageCtx, crawlID, pattern, pageSize)
		cancelPage()
		if err != nil {
			if commoncrawl.IsTransientError(err) {
				cached, cacheErr := r.Store.CachedCDXRecords(ctx, runID, source, crawlID, pattern)
				if cacheErr == nil && len(cached) > 0 {
					total += len(cached)
					if limit > 0 && total > limit {
						return fmt.Errorf("cached cdx discovery for %s %s exceeded --cdx-limit=%d; discovery would be truncated by user-configured budget", source, crawlID, limit)
					}
					if err := r.Store.StartCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize); err != nil {
						return err
					}
					if err := r.Store.AddCDXRecords(ctx, runID, source, crawlID, cached); err != nil {
						_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize, len(cached), false, err.Error())
						return err
					}
					if err := r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize, len(cached), true, "local cdx cache fallback after CDX API error"); err != nil {
						return err
					}
					continue
				}
				rawCtx, cancelRaw := context.WithTimeout(ctx, rawCDXIndexTimeout)
				records, rawErr := r.CommonCrawl.QueryCDXFromRawIndex(rawCtx, crawlID, pattern)
				cancelRaw()
				if rawErr == nil {
					filtered := records[:0]
					for _, record := range records {
						if commoncrawl.ValidCDXRecord(record) {
							filtered = append(filtered, record)
						}
					}
					total += len(filtered)
					if limit > 0 && total > limit {
						return fmt.Errorf("raw cdx discovery for %s %s exceeded --cdx-limit=%d; discovery would be truncated by user-configured budget", source, crawlID, limit)
					}
					if err := r.Store.StartCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize); err != nil {
						return err
					}
					if err := r.Store.AddCDXRecords(ctx, runID, source, crawlID, filtered); err != nil {
						_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize, len(filtered), false, err.Error())
						return err
					}
					if err := r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize, len(filtered), true, "raw index fallback after CDX API error"); err != nil {
						return err
					}
					continue
				} else {
					if cacheErr != nil {
						err = fmt.Errorf("%w; raw index fallback: %v; local cdx cache fallback: %v", err, rawErr, cacheErr)
					} else {
						err = fmt.Errorf("%w; raw index fallback: %v; local cdx cache fallback: no cached records", err, rawErr)
					}
				}
				_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, 0, pageSize, 0, false, err.Error())
				continue
			}
			return err
		}
		if limit > 0 && pageCount > (limit+pageSize-1)/pageSize+1 {
			return fmt.Errorf("cdx discovery for %s %s may exceed --cdx-limit=%d (%d pages at pageSize=%d); increase --cdx-limit or narrow the run", source, crawlID, limit, pageCount, pageSize)
		}
		for page := 0; page < pageCount; page++ {
			state, ok, err := r.Store.CDXPage(ctx, runID, source, crawlID, pattern, page)
			if err != nil {
				return err
			}
			if ok && state.State == "succeeded" {
				total += state.RecordCount
				if limit > 0 && total > limit {
					return fmt.Errorf("cdx discovery for %s %s exceeded --cdx-limit=%d during resume", source, crawlID, limit)
				}
				continue
			}
			if err := r.Store.StartCDXPage(ctx, runID, source, crawlID, pattern, page, pageSize); err != nil {
				return err
			}
			cdxCtx, cancelCDX := context.WithTimeout(ctx, cdxAPITimeout)
			records, err := r.CommonCrawl.QueryCDXPage(cdxCtx, crawlID, pattern, page, pageSize)
			cancelCDX()
			if err != nil {
				_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, page, pageSize, 0, false, err.Error())
				if commoncrawl.IsTransientError(err) {
					continue
				}
				return err
			}
			filtered := records[:0]
			for _, record := range records {
				if commoncrawl.ValidCDXRecord(record) {
					filtered = append(filtered, record)
				}
			}
			if page < pageCount-1 && len(records) == 0 {
				err := fmt.Errorf("cdx page inconsistency for %s %s page %d: empty page before final page", crawlID, pattern, page)
				_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, page, pageSize, 0, false, err.Error())
				return err
			}
			total += len(filtered)
			if limit > 0 && total > limit {
				err := fmt.Errorf("cdx discovery for %s %s exceeded --cdx-limit=%d; discovery would be truncated by user-configured budget", source, crawlID, limit)
				_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, page, pageSize, len(filtered), false, err.Error())
				return err
			}
			if err := r.Store.AddCDXRecords(ctx, runID, source, crawlID, filtered); err != nil {
				_ = r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, page, pageSize, len(filtered), false, err.Error())
				return err
			}
			if err := r.Store.CompleteCDXPage(ctx, runID, source, crawlID, pattern, page, pageSize, len(filtered), true, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

func containsMonthSelection(crawls []string) bool {
	for _, crawl := range crawls {
		crawl = strings.TrimSpace(crawl)
		if len(crawl) == len("2006-01") && crawl[4] == '-' {
			return true
		}
	}
	return false
}

func graphReleaseMetadata(catalog []commoncrawl.CatalogEntry, resolvedID string) string {
	for _, entry := range catalog {
		if entry.ID != resolvedID {
			continue
		}
		return fmt.Sprintf(`{"id":%q,"from":%q,"to":%q,"api":%q}`, entry.ID, entry.From, entry.To, entry.API)
	}
	return ""
}

func (r Runner) ProcessWARC(ctx context.Context, runID string, maxTasks int) error {
	if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
		return err
	}
	workerID := r.WorkerID
	if workerID == "" {
		workerID = "workflow"
	}
	concurrency := r.Config.Concurrency.WARC
	if concurrency <= 0 {
		concurrency = 1
	}
	tasks, err := r.Store.LeaseWARCTasks(ctx, runID, workerID, maxTasks, 5*time.Minute)
	if err != nil || len(tasks) == 0 {
		return err
	}
	jobs := make(chan storage.LeasedWARCTask)
	errs := make(chan error, len(tasks))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
					errs <- err
					continue
				}
				if err := r.Store.StartTask(ctx, runID, "warc_tasks", task.ID); err != nil {
					errs <- err
					continue
				}
				if err := r.processWARCTask(ctx, runID, task); err != nil {
					if !continueAfterTaskError(ctx, err) {
						errs <- err
					} else {
						sleepContext(ctx, taskRetryBackoff(r.Config, task.Attempts))
					}
				}
			}
		}()
	}
enqueue:
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			break enqueue
		case jobs <- task:
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r Runner) ProcessWAT(ctx context.Context, runID string, maxTasks int) error {
	if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
		return err
	}
	workerID := r.WorkerID
	if workerID == "" {
		workerID = "workflow"
	}
	concurrency := r.Config.Concurrency.WAT
	if concurrency <= 0 {
		concurrency = 1
	}
	tasks, err := r.Store.LeaseWATTasks(ctx, runID, workerID, maxTasks, 5*time.Minute)
	if err != nil || len(tasks) == 0 {
		return err
	}
	jobs := make(chan storage.LeasedWATTask)
	errs := make(chan error, len(tasks))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
					errs <- err
					continue
				}
				if err := r.Store.StartTask(ctx, runID, "wat_tasks", task.ID); err != nil {
					errs <- err
					continue
				}
				if err := r.processWATTask(ctx, runID, task); err != nil {
					if !continueAfterTaskError(ctx, err) {
						errs <- err
					} else {
						sleepContext(ctx, taskRetryBackoff(r.Config, task.Attempts))
					}
				}
			}
		}()
	}
enqueue:
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			break enqueue
		case jobs <- task:
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r Runner) ProcessGraph(ctx context.Context, runID string, maxTasks int) error {
	if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
		return err
	}
	workerID := r.WorkerID
	if workerID == "" {
		workerID = "workflow"
	}
	concurrency := r.Config.Concurrency.Graph
	if concurrency <= 0 {
		concurrency = 1
	}
	tasks, err := r.Store.LeaseGraphTasks(ctx, runID, workerID, maxTasks, 10*time.Minute)
	if err != nil || len(tasks) == 0 {
		return err
	}
	jobs := make(chan storage.LeasedGraphTask)
	errs := make(chan error, len(tasks))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
					errs <- err
					continue
				}
				if err := r.Store.StartTask(ctx, runID, "graph_tasks", task.ID); err != nil {
					errs <- err
					continue
				}
				if err := r.processGraphTask(ctx, runID, task); err != nil {
					if !continueAfterTaskError(ctx, err) {
						errs <- err
					} else {
						sleepContext(ctx, taskRetryBackoff(r.Config, task.Attempts))
					}
				}
			}
		}()
	}
enqueue:
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			break enqueue
		case jobs <- task:
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r Runner) processWARCTask(ctx context.Context, runID string, task storage.LeasedWARCTask) error {
	maxRecord := r.Config.CommonCrawl.MaxWARCRecordBytes
	if maxRecord <= 0 {
		maxRecord = 8 * 1024 * 1024
	}
	if task.Length > maxRecord {
		return r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, true, "")
	}
	data, err := r.CommonCrawl.FetchRange(ctx, task.Filename, task.Offset, task.Length)
	if err != nil {
		if finalCommonCrawlRangeError(err) {
			return r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, true, "")
		}
		_ = r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, false, err.Error())
		return err
	}
	sourceURL := task.SourceURL
	if sourceURL == "" {
		sourceURL = "https://" + task.SourceDomain + "/"
	}
	observations, err := extraction.ExtractWARCHTMLWithLimit(data, extraction.HTMLContext{
		SourceDomain: task.SourceDomain,
		SourceURL:    sourceURL,
		CrawlID:      task.CrawlID,
		Timestamp:    task.Timestamp,
		WARCFile:     task.Filename,
		WARCOffset:   task.Offset,
		WARCLength:   task.Length,
	}, r.Config.CommonCrawl.MaxWARCDecodedBytes)
	if err != nil {
		if finalParseError(err) {
			return r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, true, "")
		}
		_ = r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, false, err.Error())
		return err
	}
	if len(observations) > 0 {
		manifest, err := r.writeLinkArtifact(runID, fmt.Sprintf("warc-%d.parquet", task.ID), observations)
		if err != nil {
			_ = r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, false, err.Error())
			return err
		}
		batchKey := fmt.Sprintf("warc:%d:%s:%s:%d:%d", task.ID, task.CrawlID, task.Filename, task.Offset, task.Length)
		if err := r.Store.AddLinkObservationBatch(ctx, runID, batchKey, observations, r.Config, manifest); err != nil {
			_ = r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, false, err.Error())
			return err
		}
	}
	if err := r.Store.CompleteTask(ctx, runID, "warc_tasks", task.ID, true, ""); err != nil {
		return err
	}
	return nil
}

func (r Runner) processWATTask(ctx context.Context, runID string, task storage.LeasedWATTask) error {
	body, err := r.CommonCrawl.FetchObject(ctx, task.WATFilename, r.Config.CommonCrawl.MaxWATObjectBytes)
	if err != nil {
		if isMissingCommonCrawlObject(err) || finalCommonCrawlObjectError(err) {
			return r.Store.CompleteTask(ctx, runID, "wat_tasks", task.ID, true, "")
		}
		_ = r.Store.CompleteTask(ctx, runID, "wat_tasks", task.ID, false, err.Error())
		return err
	}
	defer body.Close()
	ctxWAT := extraction.WATContext{
		SourceDomain:  task.SourceDomain,
		SourceDomains: map[string]bool{task.SourceDomain: true},
		CrawlID:       task.CrawlID,
	}
	opts := extraction.WATScanOptions{
		MaxLineBytes:  10 * 1024 * 1024,
		MaxTotalBytes: r.Config.CommonCrawl.MaxWATObjectBytes,
		BatchRows:     r.Config.CommonCrawl.WATBatchRows,
	}
	err = extraction.ScanWATBatches(body, ctxWAT, opts, func(batchIndex int, observations []extraction.LinkObservation) error {
		if len(observations) == 0 {
			return nil
		}
		manifest, err := r.writeLinkArtifact(runID, fmt.Sprintf("wat-%d-%d.parquet", task.ID, batchIndex), observations)
		if err != nil {
			return err
		}
		batchKey := fmt.Sprintf("wat:%d:%d:%s:%s", task.ID, batchIndex, task.CrawlID, task.WATFilename)
		return r.Store.AddLinkObservationBatch(ctx, runID, batchKey, observations, r.Config, manifest)
	})
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "wat_tasks", task.ID, false, err.Error())
		return err
	}
	return r.Store.CompleteTask(ctx, runID, "wat_tasks", task.ID, true, "")
}

func finalParseError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "missing warc version") ||
		strings.Contains(message, "truncated warc") ||
		strings.Contains(message, "oversized warc") ||
		strings.Contains(message, "malformed")
}

func isMissingCommonCrawlObject(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "http 404")
}

func finalCommonCrawlRangeError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "common crawl range http 403") ||
		strings.Contains(message, "common crawl range http 404") ||
		strings.Contains(message, "common crawl range http 416")
}

func finalCommonCrawlObjectError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "common crawl object http 403") ||
		strings.Contains(message, "common crawl object http 416")
}

func (r Runner) processGraphTask(ctx context.Context, runID string, task storage.LeasedGraphTask) error {
	maxGraphBytes := r.Config.CommonCrawl.MaxGraphObjectBytes
	vertexBody, err := r.CommonCrawl.FetchObject(ctx, task.VertexPath, maxGraphBytes)
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	sourceSet := map[string]bool{}
	for _, source := range task.SourceDomains {
		sourceSet[source] = true
	}
	sourceIDs, err := graph.ScanVertices(vertexBody, sourceSet)
	vertexBody.Close()
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	edgeBody, err := r.CommonCrawl.FetchObject(ctx, task.FilePath, maxGraphBytes)
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	sourceEdges, err := graph.ScanSourceEdges(edgeBody, sourceIDs)
	edgeBody.Close()
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	vertexBody, err = r.CommonCrawl.FetchObject(ctx, task.VertexPath, maxGraphBytes)
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	targetResolver, err := graph.ResolveVertices(vertexBody, graph.TargetIDs(sourceEdges))
	vertexBody.Close()
	if err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	observations := graph.ObservationsFromSourceEdges(sourceEdges, targetResolver, task.ReleaseID)
	batchKey := fmt.Sprintf("graph:%d:%s:%s", task.ID, task.ReleaseID, task.FilePath)
	if err := r.Store.AddGraphObservationBatch(ctx, runID, batchKey, observations, r.Config); err != nil {
		_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
		return err
	}
	if len(observations) > 0 {
		runDir, err := r.Store.RunDir(runID)
		if err != nil {
			_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
			return err
		}
		manifest, err := artifact.WriteGraphObservations(filepath.Join(runDir, "artifacts", "graph_candidates", fmt.Sprintf("graph-%d.parquet", task.ID)), observations)
		if err != nil {
			_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
			return err
		}
		if err := r.Store.RecordArtifact(ctx, runID, "graph_candidates", manifest); err != nil {
			_ = r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, false, err.Error())
			return err
		}
	}
	return r.Store.CompleteTask(ctx, runID, "graph_tasks", task.ID, true, "")
}

func (r Runner) writeLinkArtifact(runID, name string, observations []extraction.LinkObservation) (artifact.Manifest, error) {
	runDir, err := r.Store.RunDir(runID)
	if err != nil {
		return artifact.Manifest{}, err
	}
	manifest, err := artifact.WriteLinkObservations(filepath.Join(runDir, "artifacts", "link_observations", name), observations)
	if err != nil {
		return artifact.Manifest{}, err
	}
	return manifest, nil
}

func (r Runner) CheckAvailability(ctx context.Context, runID string, limit int) error {
	if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
		return err
	}
	workerID := r.WorkerID
	if workerID == "" {
		workerID = "workflow"
	}
	tasks, err := r.Store.LeaseAvailabilityTasks(ctx, runID, workerID, limit, 5*time.Minute)
	if err != nil || len(tasks) == 0 {
		return err
	}
	concurrency := r.Config.Concurrency.DNS
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(tasks) {
		concurrency = len(tasks)
	}
	rdapLimit := r.Config.Concurrency.RDAP
	if rdapLimit <= 0 {
		rdapLimit = 1
	}
	registrarLimit := r.Config.Concurrency.Registrar
	if registrarLimit <= 0 {
		registrarLimit = 1
	}
	rdapSem := make(chan struct{}, rdapLimit)
	registrarSem := make(chan struct{}, registrarLimit)
	jobs := make(chan storage.LeasedAvailabilityTask)
	errs := make(chan error, len(tasks))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := r.Store.EnsureRunnable(ctx, runID); err != nil {
					errs <- err
					continue
				}
				if err := r.Store.StartTask(ctx, runID, "availability_tasks", task.ID); err != nil {
					errs <- fmt.Errorf("availability start task %s: %w", task.Domain, err)
					continue
				}
				if err := r.checkDomain(ctx, runID, task.Domain, rdapSem, registrarSem); err != nil {
					var deferred availabilityDeferredError
					if errors.As(err, &deferred) {
						retryAt := time.Now().UTC().Add(deferred.retryAfter)
						if err := r.Store.DeferAvailabilityTask(ctx, runID, task.ID, retryAt, deferred.Error()); err != nil {
							errs <- fmt.Errorf("availability defer task %s: %w", task.Domain, err)
						}
						continue
					}
					if completeErr := r.Store.CompleteTask(ctx, runID, "availability_tasks", task.ID, false, err.Error()); completeErr != nil {
						errs <- fmt.Errorf("availability complete failed task %s: %w", task.Domain, completeErr)
					}
					sleepContext(ctx, taskRetryBackoff(r.Config, task.Attempts))
					continue
				}
				if err := r.Store.CompleteTask(ctx, runID, "availability_tasks", task.ID, true, ""); err != nil {
					errs <- fmt.Errorf("availability complete task %s: %w", task.Domain, err)
				}
			}
		}()
	}
enqueue:
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			break enqueue
		case jobs <- task:
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func continueAfterTaskError(ctx context.Context, err error) bool {
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	return commoncrawl.IsTransientError(err)
}

func taskRetryBackoff(cfg config.Config, attempt int) time.Duration {
	base := time.Duration(cfg.Runtime.InitialRetryBackoffMillis) * time.Millisecond
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	maxDelay := time.Duration(cfg.Runtime.MaxRetryBackoffSeconds) * time.Second
	if maxDelay <= 0 {
		maxDelay = 60 * time.Second
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) {
	if delay <= 0 {
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type availabilityDeferredError struct {
	retryAfter time.Duration
	err        error
}

func (e availabilityDeferredError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return "availability task deferred"
}

func (e availabilityDeferredError) Unwrap() error {
	return e.err
}

func (r Runner) checkDomain(ctx context.Context, runID, domain string, rdapSem, registrarSem chan struct{}) error {
	now := time.Now().UTC()
	ttlPolicy := availabilityTTLPolicy(r.Config)
	if cached, ok, err := r.Store.CachedAvailability(ctx, domain, now); err != nil {
		return err
	} else if ok {
		cached.CacheHit = true
		return r.Store.StoreAvailability(ctx, runID, cached)
	}
	tldPolicy, policySignals := classifyTLDPolicy(domain, r.Config)
	if cachedPolicy, ok, err := r.Store.TLDPolicyCategory(ctx, domain); err != nil {
		return err
	} else if ok {
		tldPolicy = cachedPolicy
		policySignals = signalsForTLDPolicy(cachedPolicy)
	}
	if policySignals.RestrictedTLD || policySignals.UnsupportedTLD || policySignals.ReservedTLD {
		classification := availability.Classify(policySignals)
		return r.Store.StoreAvailability(ctx, runID, storage.AvailabilityRecord{
			Domain:            domain,
			Status:            classification.Status,
			Confidence:        classification.Confidence,
			TLDPolicyCategory: tldPolicy,
			Reason:            "classified from tld policy before network checks",
			CheckedAt:         now,
			ExpiresAt:         now.Add(ttlPolicy.TTL(classification.Status)),
		})
	}
	dnsResolver := r.DNSResolver
	if dnsResolver == nil && len(r.Config.Availability.DNSResolvers) > 0 {
		dnsResolver = availability.NewDNSResolver(r.Config.Availability.DNSResolvers)
	}
	dns := availability.CheckDNS(ctx, dnsResolver, domain)
	rdapClient := r.RDAP
	if rdapClient.UserAgent == "" {
		rdapClient.UserAgent = r.Config.CommonCrawl.UserAgent
	}
	if err := acquire(ctx, rdapSem); err != nil {
		return err
	}
	rdap, err := rdapClient.Lookup(ctx, domain)
	release(rdapSem)
	signals := availability.SignalsFromDNSRDAP(dns, rdap)
	signals.RestrictedTLD = policySignals.RestrictedTLD
	signals.UnsupportedTLD = policySignals.UnsupportedTLD
	signals.ReservedTLD = policySignals.ReservedTLD
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "rate") {
			signals.RateLimited = true
		} else {
			signals.Error = true
		}
	}
	provider := ""
	premium := false
	priceCents := 0
	currency := ""
	preliminary := availability.Classify(signals)
	if r.Registrar != nil && !signals.RateLimited && shouldCheckRegistrar(preliminary.Status, r.Config) {
		if err := acquire(ctx, registrarSem); err != nil {
			return err
		}
		result, err := r.Registrar.Check(ctx, domain)
		release(registrarSem)
		if err != nil {
			var rateLimit availability.RateLimitError
			if errors.As(err, &rateLimit) || strings.Contains(strings.ToLower(err.Error()), "rate") {
				retryAfter := rateLimit.RetryAfter
				if retryAfter <= 0 {
					retryAfter = availabilityTTLPolicy(r.Config).RateLimited
				}
				return availabilityDeferredError{retryAfter: retryAfter, err: err}
			} else if !signals.RDAPFound && !signals.RDAPNotFound {
				signals.Error = true
			}
		} else {
			provider = r.Registrar.Name()
			signals.RegistrarAvailable = result.Available
			signals.RegistrarPremium = result.Premium
			premium = result.Premium
			priceCents = result.PriceCents
			currency = result.Currency
		}
	}
	classification := availability.Classify(signals)
	record := storage.AvailabilityRecord{
		Domain:            domain,
		Status:            classification.Status,
		Confidence:        classification.Confidence,
		TLDPolicyCategory: tldPolicy,
		Reason:            "classified from dns rdap registrar signals",
		Provider:          provider,
		Premium:           premium,
		PriceCents:        priceCents,
		Currency:          currency,
		CheckedAt:         now,
		ExpiresAt:         now.Add(ttlPolicy.TTL(classification.Status)),
	}
	return r.Store.StoreAvailability(ctx, runID, record)
}

func shouldCheckRegistrar(status availability.Status, cfg config.Config) bool {
	statuses := cfg.Availability.RegistrarCheckStatuses
	if len(statuses) == 0 {
		statuses = []string{string(availability.AvailableLikely)}
	}
	for _, configured := range statuses {
		configured = strings.TrimSpace(configured)
		if configured == "*" || strings.EqualFold(configured, string(status)) {
			return true
		}
	}
	return false
}

func availabilityTTLPolicy(cfg config.Config) availability.TTLPolicy {
	policy := availability.DefaultTTLPolicy()
	if cfg.Cache.RegisteredTTLDays > 0 {
		policy.Registered = time.Duration(cfg.Cache.RegisteredTTLDays) * 24 * time.Hour
	}
	if cfg.Cache.AvailableTTLHours > 0 {
		policy.Available = time.Duration(cfg.Cache.AvailableTTLHours) * time.Hour
	}
	if cfg.Cache.LikelyAvailableTTLHrs > 0 {
		policy.Likely = time.Duration(cfg.Cache.LikelyAvailableTTLHrs) * time.Hour
	}
	if cfg.Cache.AmbiguousTTLHours > 0 {
		policy.Ambiguous = time.Duration(cfg.Cache.AmbiguousTTLHours) * time.Hour
	}
	if cfg.Cache.UnsupportedTLDTtlDays > 0 {
		policy.UnsupportedTLD = time.Duration(cfg.Cache.UnsupportedTLDTtlDays) * 24 * time.Hour
	}
	return policy
}

func acquire(ctx context.Context, sem chan struct{}) error {
	if sem == nil {
		return nil
	}
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func release(sem chan struct{}) {
	if sem == nil {
		return
	}
	<-sem
}

func ValidateBoundedConcurrency(cfg config.Config) error {
	for name, value := range map[string]int{
		"cdx":       cfg.Concurrency.CDX,
		"warc":      cfg.Concurrency.WARC,
		"wat":       cfg.Concurrency.WAT,
		"graph":     cfg.Concurrency.Graph,
		"dns":       cfg.Concurrency.DNS,
		"rdap":      cfg.Concurrency.RDAP,
		"registrar": cfg.Concurrency.Registrar,
	} {
		if value <= 0 {
			return fmt.Errorf("%s concurrency must be positive", name)
		}
	}
	if cfg.Runtime.QueueSize <= 0 {
		return fmt.Errorf("queue size must be positive")
	}
	return nil
}

func classifyTLDPolicy(domain string, cfg config.Config) (string, availability.Signals) {
	tld := domain
	if i := strings.LastIndexByte(domain, '.'); i >= 0 {
		tld = domain[i+1:]
	}
	for _, restricted := range cfg.Availability.RestrictedTLDs {
		if strings.EqualFold(tld, strings.TrimPrefix(restricted, ".")) {
			return "restricted", availability.Signals{RestrictedTLD: true}
		}
	}
	for _, blocked := range cfg.Availability.BlockedTLDs {
		if strings.EqualFold(tld, strings.TrimPrefix(blocked, ".")) {
			return "unsupported_unknown", availability.Signals{UnsupportedTLD: true}
		}
	}
	switch strings.ToLower(tld) {
	case "arpa", "localhost", "example", "invalid", "test":
		return "reserved_infrastructure", availability.Signals{ReservedTLD: true}
	default:
		return "open_retail", availability.Signals{}
	}
}

func signalsForTLDPolicy(category string) availability.Signals {
	switch category {
	case "restricted":
		return availability.Signals{RestrictedTLD: true}
	case "unsupported_unknown":
		return availability.Signals{UnsupportedTLD: true}
	case "reserved_infrastructure":
		return availability.Signals{ReservedTLD: true}
	default:
		return availability.Signals{}
	}
}
