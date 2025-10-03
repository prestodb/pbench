package stage

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"pbench/log"
	"regexp"
	"strings"
	"time"
)

const PulumiAPIEndpoint = "https://api.pulumi.com"
const PulumiResourceTypeStack = "pulumi:pulumi:Stack"

//go:embed pbench_clusters_ddl.sql
var pbenchClustersDDL string

// PulumiMySQLRunRecorder will record the cluster information from Pulumi to MySQL for correlative analysis in Grafana
type PulumiMySQLRunRecorder struct {
	Token        string `json:"token"`
	Organization string `json:"organization"`
	Project      string `json:"project"`
	db           *sql.DB
	apiEndpoint  *url.URL
}

func NewPulumiMySQLRunRecorder(cfgPath string, mySQLRunRecorder *MySQLRunRecorder) *PulumiMySQLRunRecorder {
	if cfgPath == "" {
		return nil
	} else if mySQLRunRecorder == nil {
		log.Error().Msg("Pulumi API config path was specified but no MySQL run recorder was initialized." +
			" Cluster deployment information cannot be stored without a MySQL run recorder.")
		return nil
	}
	if bytes, ioErr := os.ReadFile(cfgPath); ioErr != nil {
		log.Error().Err(ioErr).Msg("failed to read Pulumi API config")
		return nil
	} else {
		recorder := &PulumiMySQLRunRecorder{}
		if err := json.Unmarshal(bytes, recorder); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal Pulumi API config for the run recorder")
			return nil
		}
		if parsedEndpoint, err := url.Parse(PulumiAPIEndpoint); err != nil {
			log.Error().Err(err).Str("endpoint", PulumiAPIEndpoint).Msg("failed to parse PulumiAPIEndpoint")
			return nil
		} else {
			recorder.apiEndpoint = parsedEndpoint
		}
		recorder.db = mySQLRunRecorder.db
		_, err := recorder.db.Exec(pbenchClustersDDL)
		if err != nil {
			log.Error().Err(err).Msg("failed to create MySQL table pbench_clusters")
			return nil
		}
		log.Info().Msg("Pulumi API config initialized, cluster deployment details will be stored to the MySQL database.")
		return recorder
	}
}

func (p *PulumiMySQLRunRecorder) newGetRequest(urlStr string) (*http.Request, error) {
	u, err := p.apiEndpoint.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "token "+p.Token)
	return req, nil
}

func (p *PulumiMySQLRunRecorder) doRequest(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// If we got an error, and the context has been canceled,
		// the context's error is probably more useful.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		switch v := v.(type) {
		case nil:
		case io.Writer:
			_, err = io.Copy(v, resp.Body)
		default:
			decErr := json.NewDecoder(resp.Body).Decode(v)
			if decErr == io.EOF {
				decErr = nil // ignore EOF errors caused by empty response body
			}
			if decErr != nil {
				err = decErr
			}
		}
		_ = resp.Body.Close()
	}
	return resp, err
}

func (p *PulumiMySQLRunRecorder) findPulumiStackFromClusterFQDN(ctx context.Context, clusterFQDN string) *PulumiResource {
	fqdnBuildNumberExtractor := regexp.MustCompile(`.+(b\d+[nj])\.ibm\.prestodb\.dev`)
	stackNameBuildNumberExtractor := regexp.MustCompile(`.+-(b\d+[nj])-.+`)

	if match := fqdnBuildNumberExtractor.FindStringSubmatch(clusterFQDN); len(match) > 0 {
		buildNumber := match[1]

		// List all the stacks.
		listStacksUrl := fmt.Sprintf("/api/user/stacks?organization=%s&tagName=pulumi:project&tagValue=%s",
			p.Organization, p.Project)
		req, err := p.newGetRequest(listStacksUrl)
		if err != nil {
			log.Error().Err(err).Msg("failed to create list stack request.")
			return nil
		}

		stacks := &PulumiStacks{}
		_, err = p.doRequest(ctx, req, stacks)
		if err != nil {
			log.Error().Err(err).Msg("failed to list Pulumi stacks.")
			return nil
		}

		for _, stack := range stacks.Stacks {
			if match = stackNameBuildNumberExtractor.FindStringSubmatch(stack.StackName); len(match) == 0 || buildNumber != match[1] {
				continue
			}
			getStackExportUrl := fmt.Sprintf("/api/stacks/%s/%s/%s/export", p.Organization, p.Project, stack.StackName)
			req, err = p.newGetRequest(getStackExportUrl)
			if err != nil {
				log.Error().Err(err).Str("stack_name", stack.StackName).Msg("failed to create get stack export request.")
				return nil
			}

			export := &PulumiStackExport{}
			_, err = p.doRequest(ctx, req, export)
			if err != nil {
				log.Error().Err(err).Str("stack_name", stack.StackName).Msg("failed to get Pulumi stack export.")
				return nil
			}

			for _, resource := range export.Deployment.Resources {
				if resource.Type != PulumiResourceTypeStack {
					continue
				}
				if resource.Outputs.ClusterFQDN == clusterFQDN {
					return &resource
				}
			}
		}
	} else {
		log.Error().Str("cluster_fqdn", clusterFQDN).Msg("failed to extract a build number from cluster FQDN")
	}
	return nil
}

func (p *PulumiMySQLRunRecorder) findStackFromClusterFQDN(ctx context.Context, clusterFQDN string) *PulumiResource {
	// Check if the cluster FQDN matches the pattern XXX.ibm.prestodb.dev
	// where XXX contains no dots (Pulumi-managed clusters)
	if strings.HasSuffix(clusterFQDN, ".ibm.prestodb.dev") {
		// Extract the prefix before .ibm.prestodb.dev
		prefix := strings.TrimSuffix(clusterFQDN, ".ibm.prestodb.dev")

		// If the prefix contains no dots, it's a Pulumi-managed cluster, oss
		// Examples: abcdef.ibm.prestodb.dev (Pulumi), abc.def.ibm.prestodb.dev (Non-Pulumi)
		if !strings.Contains(prefix, ".") {
			return p.findPulumiStackFromClusterFQDN(ctx, clusterFQDN)
		}
	}

	// All other patterns (including blueray/wxd are non-Pulumi managed
	return p.findNonPulumiStackFromClusterFQDN(clusterFQDN)
}

func (p *PulumiMySQLRunRecorder) findNonPulumiStackFromClusterFQDN(clusterFQDN string) *PulumiResource {
	// Extract cluster name as the first part of the FQDN (before the first dot)
	parts := strings.SplitN(clusterFQDN, ".", 2)
	if len(parts) < 2 {
		log.Error().Str("cluster_fqdn", clusterFQDN).Msg("failed to extract cluster name from Blueray FQDN")
		return nil
	}

	clusterName := parts[0]

	// Create a PulumiResource with the extracted information
	resource := &PulumiResource{
		Type:    PulumiResourceTypeStack,
		Created: time.Now(),
	}

	// Set the outputs
	resource.Outputs.ClusterFQDN = clusterFQDN
	resource.Outputs.ClusterName = clusterName

	log.Info().Str("cluster_name", clusterName).Str("cluster_fqdn", clusterFQDN).
		Msg("extracted cluster information from Blueray FQDN")

	return resource
}

func (p *PulumiMySQLRunRecorder) Start(ctx context.Context, s *Stage) error {
	stack := p.findStackFromClusterFQDN(ctx, s.States.ServerFQDN)
	if stack == nil {
		log.Info().Msgf("did not find a matching Pulumi stack for %s", s.States.ServerFQDN)
		return nil
	}
	log.Info().Str("cluster_name", stack.Outputs.ClusterName).Str("cluster_fqdn", s.States.ServerFQDN).
		Msg("recorded cluster information")
	recordCluster := `INSERT IGNORE INTO pbench_clusters (cluster_name, cluster_fqdn, created) VALUES (?, ?, ?)`
	_, err := p.db.Exec(recordCluster, stack.Outputs.ClusterName, s.States.ServerFQDN, stack.Created)
	if err != nil {
		log.Error().Err(err).Str("run_name", s.States.RunName).Str("cluster_fqdn", s.States.ServerFQDN).
			Msg("failed to record cluster info")
		return err
	}
	return nil
}

func (p *PulumiMySQLRunRecorder) RecordRun(_ context.Context, _ *Stage, _ []*QueryResult) {}

func (p *PulumiMySQLRunRecorder) RecordQuery(_ context.Context, _ *Stage, _ *QueryResult) {}

type PulumiStacks struct {
	Stacks []struct {
		OrgName     string `json:"orgName"`
		ProjectName string `json:"projectName"`
		StackName   string `json:"stackName"`
	} `json:"stacks"`
}

type PulumiResource struct {
	Type    string `json:"type"`
	Outputs struct {
		ClusterFQDN string `json:"cluster_fqdn"`
		ClusterName string `json:"cluster_name"`
	} `json:"outputs"`
	Created time.Time `json:"created"`
}

type PulumiStackExport struct {
	Deployment struct {
		Resources []PulumiResource `json:"resources"`
	} `json:"deployment"`
}
