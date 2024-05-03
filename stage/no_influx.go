//go:build !influx

package stage

func NewInfluxRunRecorder(_ string) RunRecorder {
	return nil
}
