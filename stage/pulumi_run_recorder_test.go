package stage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindNonPulumiStackFromClusterFQDN(t *testing.T) {
	recorder := &PulumiMySQLRunRecorder{}

	tests := []struct {
		name                string
		fqdn                string
		expectedClusterName string
	}{
		{
			name:                "Blueray/WXD cluster",
			fqdn:                "xlarge-b109n-yabin-eng.k9b9rz3nk2.staging.cvpc.lakehouse.test.cloud.ibm.com",
			expectedClusterName: "xlarge-b109n-yabin-eng",
		},
		{
			name:                "non-Pulumi with ibm.prestodb.dev suffix",
			fqdn:                "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance.apps.demokai530.ibm.prestodb.dev",
			expectedClusterName: "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance",
		},
		{
			name:                "local cluster service",
			fqdn:                "ibm-lh-lakehouse-mds-thrift-svc.cpd-instance.svc.cluster.local",
			expectedClusterName: "ibm-lh-lakehouse-mds-thrift-svc",
		},
		{
			name: "single-label FQDN returns nil",
			fqdn: "no-dots-hostname",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resource := recorder.findNonPulumiStackFromClusterFQDN(tc.fqdn)
			if tc.expectedClusterName == "" {
				assert.Nil(t, resource)
				return
			}
			require.NotNil(t, resource)
			assert.Equal(t, tc.expectedClusterName, resource.Outputs.ClusterName)
			assert.Equal(t, tc.fqdn, resource.Outputs.ClusterFQDN)
			assert.Equal(t, PulumiResourceTypeStack, resource.Type)
			assert.False(t, resource.Created.IsZero())
		})
	}
}

func TestFindStackFromClusterFQDN_Routing(t *testing.T) {
	// findStackFromClusterFQDN routes to findPulumiStackFromClusterFQDN for Pulumi-managed
	// clusters and findNonPulumiStackFromClusterFQDN for everything else. We can't test the
	// Pulumi path without a real API, but we can verify that non-Pulumi FQDNs get routed
	// correctly by checking that they return a synthesized resource (no API call needed).
	recorder := &PulumiMySQLRunRecorder{}
	ctx := context.Background()

	tests := []struct {
		name                string
		fqdn                string
		expectedClusterName string
		expectNil           bool
	}{
		{
			// Pulumi-managed: <no-dots>.ibm.prestodb.dev — routes to Pulumi API which
			// returns nil without valid credentials, so we expect nil here.
			name:      "Pulumi-managed cluster returns nil without API",
			fqdn:      "xlarge-b109n-yabin-eng.ibm.prestodb.dev",
			expectNil: true,
		},
		{
			name:                "non-Pulumi with dots before ibm.prestodb.dev",
			fqdn:                "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance.apps.demokai530.ibm.prestodb.dev",
			expectedClusterName: "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance",
		},
		{
			name:                "Blueray/WXD cluster",
			fqdn:                "xlarge-b109n-yabin-eng.k9b9rz3nk2.staging.cvpc.lakehouse.test.cloud.ibm.com",
			expectedClusterName: "xlarge-b109n-yabin-eng",
		},
		{
			name:                "local cluster service",
			fqdn:                "ibm-lh-lakehouse-mds-thrift-svc.cpd-instance.svc.cluster.local",
			expectedClusterName: "ibm-lh-lakehouse-mds-thrift-svc",
		},
		{
			name:      "single-label FQDN",
			fqdn:      "no-dots-hostname",
			expectNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resource := recorder.findStackFromClusterFQDN(ctx, tc.fqdn)
			if tc.expectNil {
				assert.Nil(t, resource)
				return
			}
			require.NotNil(t, resource)
			assert.Equal(t, tc.expectedClusterName, resource.Outputs.ClusterName)
			assert.Equal(t, tc.fqdn, resource.Outputs.ClusterFQDN)
		})
	}
}
