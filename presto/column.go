package presto

type Column struct {
	Name          string              `json:"name"`
	Type          string              `json:"type"`
	TypeSignature ClientTypeSignature `json:"typeSignature"`
}
