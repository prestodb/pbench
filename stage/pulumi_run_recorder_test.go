package stage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindBluerayStackFromClusterFQDN(t *testing.T) {
	// Create a recorder with minimal initialization for testing
	recorder := &PulumiMySQLRunRecorder{}

	// Test FQDN
	testFQDN := "xlarge-b109n-yabin-eng.k9b9rz3nk2.staging.cvpc.lakehouse.test.cloud.ibm.com"
	expectedClusterName := "xlarge-b109n-yabin-eng"

	// Call the function
	resource := recorder.findBluerayStackFromClusterFQDN(testFQDN)

	// Verify resource is not nil
	require.NotNil(t, resource)

	// Verify the cluster FQDN
	assert.Equal(t, testFQDN, resource.Outputs.ClusterFQDN)

	// Verify the cluster name
	assert.Equal(t, expectedClusterName, resource.Outputs.ClusterName)

	// Verify the resource type
	assert.Equal(t, PulumiResourceTypeStack, resource.Type)

	// Verify Created timestamp is not zero
	assert.False(t, resource.Created.IsZero())
}
