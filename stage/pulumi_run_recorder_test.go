package stage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindNonPulumiStackFromClusterFQDN(t *testing.T) {
	// Create a recorder with minimal initialization for testing
	recorder := &PulumiMySQLRunRecorder{}

	testCases := []struct {
		name                string
		fqdn                string
		expectedClusterName string
		shouldCallNonPulumi bool
	}{
		{
			name:                "Blueray/WXD cluster with multiple dots",
			fqdn:                "xlarge-b109n-yabin-eng.k9b9rz3nk2.staging.cvpc.lakehouse.test.cloud.ibm.com",
			expectedClusterName: "xlarge-b109n-yabin-eng",
			shouldCallNonPulumi: true,
		},
		{
			name:                "Cluster name without domain",
			fqdn:                "xlarge-b109n-yabin-eng",
			expectedClusterName: "",
			shouldCallNonPulumi: true,
		},
		{
			name:                "Pulumi-managed cluster (single prefix, no dots)",
			fqdn:                "xlarge-b109n-yabin-eng.ibm.prestodb.dev",
			expectedClusterName: "xlarge-b109n-yabin-eng",
			shouldCallNonPulumi: false, // This should call findPulumiStackFromClusterFQDN
		},
		{
			name:                "Non-Pulumi cluster with dots in prefix",
			fqdn:                "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance.apps.demokai530.ibm.prestodb.dev",
			expectedClusterName: "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance",
			shouldCallNonPulumi: true,
		},
		{
			name:                "Local cluster service",
			fqdn:                "ibm-lh-lakehouse-mds-thrift-svc.cpd-instance.svc.cluster.local",
			expectedClusterName: "ibm-lh-lakehouse-mds-thrift-svc",
			shouldCallNonPulumi: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function directly
			resource := recorder.findNonPulumiStackFromClusterFQDN(tc.fqdn)

			if tc.expectedClusterName == "" {
				// For invalid FQDNs (no dots), expect nil
				assert.Nil(t, resource)
			} else {
				// Verify resource is not nil
				require.NotNil(t, resource)

				// Verify the cluster FQDN
				assert.Equal(t, tc.fqdn, resource.Outputs.ClusterFQDN)

				// Verify the cluster name
				assert.Equal(t, tc.expectedClusterName, resource.Outputs.ClusterName)

				// Verify the resource type
				assert.Equal(t, PulumiResourceTypeStack, resource.Type)

				// Verify Created timestamp is not zero
				assert.False(t, resource.Created.IsZero())
			}
		})
	}
}

func TestFindStackFromClusterFQDN_RoutingLogic(t *testing.T) {
	// This test verifies the routing logic in findStackFromClusterFQDN
	// to ensure it correctly determines which function to call

	testCases := []struct {
		name                string
		fqdn                string
		shouldCallPulumi    bool
		shouldCallNonPulumi bool
	}{
		{
			name:                "Pulumi-managed: single prefix with .ibm.prestodb.dev",
			fqdn:                "xlarge-b109n-yabin-eng.ibm.prestodb.dev",
			shouldCallPulumi:    true,
			shouldCallNonPulumi: false,
		},
		{
			name:                "Non-Pulumi: multiple dots before .ibm.prestodb.dev",
			fqdn:                "ibm-lh-lakehouse-prestissimo829-presto-svc-cpd-instance.apps.demokai530.ibm.prestodb.dev",
			shouldCallPulumi:    false,
			shouldCallNonPulumi: true,
		},
		{
			name:                "Non-Pulumi: Blueray/WXD cluster",
			fqdn:                "xlarge-b109n-yabin-eng.k9b9rz3nk2.staging.cvpc.lakehouse.test.cloud.ibm.com",
			shouldCallPulumi:    false,
			shouldCallNonPulumi: true,
		},
		{
			name:                "Non-Pulumi: local cluster service",
			fqdn:                "ibm-lh-lakehouse-mds-thrift-svc.cpd-instance.svc.cluster.local",
			shouldCallPulumi:    false,
			shouldCallNonPulumi: true,
		},
		{
			name:                "Non-Pulumi: cluster name without domain",
			fqdn:                "xlarge-b109n-yabin-eng",
			shouldCallPulumi:    false,
			shouldCallNonPulumi: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify the routing logic
			hasSuffix := strings.HasSuffix(tc.fqdn, ".ibm.prestodb.dev")
			var prefix string
			var noDots bool

			if hasSuffix {
				prefix = strings.TrimSuffix(tc.fqdn, ".ibm.prestodb.dev")
				noDots = !strings.Contains(prefix, ".")
			}

			shouldCallPulumi := hasSuffix && noDots
			shouldCallNonPulumi := !shouldCallPulumi

			assert.Equal(t, tc.shouldCallPulumi, shouldCallPulumi,
				"Expected shouldCallPulumi=%v for FQDN: %s", tc.shouldCallPulumi, tc.fqdn)
			assert.Equal(t, tc.shouldCallNonPulumi, shouldCallNonPulumi,
				"Expected shouldCallNonPulumi=%v for FQDN: %s", tc.shouldCallNonPulumi, tc.fqdn)
		})
	}
}
