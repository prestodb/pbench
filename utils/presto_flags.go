package utils

import (
	presto "github.com/ethanyzhang/presto-go"

	"github.com/spf13/cobra"
)

const DefaultServerUrl = "http://127.0.0.1:8080"

type NewPrestoClientFn func() *presto.Client

type PrestoFlags struct {
	ServerUrl  string
	IsTrino    bool
	ForceHttps bool
	UserName   string
	Password   string
}

func (pf *PrestoFlags) Install(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&pf.ServerUrl, "server", "s", DefaultServerUrl, "Presto server address")
	cmd.Flags().BoolVarP(&pf.IsTrino, "trino", "", false, "Use Trino protocol")
	cmd.Flags().BoolVarP(&pf.ForceHttps, "force-https", "", false, "Force all API requests to use HTTPS")
	cmd.Flags().StringVarP(&pf.UserName, "user", "u", presto.DefaultUser, "Presto user name")
	cmd.Flags().StringVarP(&pf.Password, "password", "p", "", "Presto user password (optional)")
}

func (pf *PrestoFlags) NewPrestoClient() *presto.Client {
	client, _ := presto.NewClient(pf.ServerUrl)
	client.IsTrino(pf.IsTrino)
	if pf.UserName != "" {
		if pf.Password != "" {
			client.UserPassword(pf.UserName, pf.Password)
		} else {
			client.User(pf.UserName)
		}
	}
	if pf.ForceHttps {
		client.ForceHTTPS(true)
	}
	return client
}

type PrestoFlagsArray struct {
	ServerUrl  []string
	IsTrino    []bool
	ForceHttps []bool
	UserName   []string
	Password   []string
}

func (a *PrestoFlagsArray) Install(cmd *cobra.Command) {
	cmd.Flags().StringArrayVarP(&a.ServerUrl, "server", "s", []string{DefaultServerUrl}, "Presto server address")
	cmd.Flags().BoolSliceVarP(&a.IsTrino, "trino", "", []bool{false}, "Use Trino protocol")
	cmd.Flags().BoolSliceVarP(&a.ForceHttps, "force-https", "", []bool{false}, "Force all API requests to use HTTPS")
	cmd.Flags().StringArrayVarP(&a.UserName, "user", "u", []string{presto.DefaultUser}, "Presto user name")
	cmd.Flags().StringArrayVarP(&a.Password, "password", "p", []string{""}, "Presto user password (optional)")
}

// Pivot generates PrestoFlags array that is suitable for creating Presto clients conveniently.
func (a *PrestoFlagsArray) Pivot() []PrestoFlags {
	ret := make([]PrestoFlags, 0, len(a.ServerUrl))
	for _, url := range a.ServerUrl {
		ret = append(ret, PrestoFlags{
			ServerUrl: url,
		})
	}
	for i, isTrino := range a.IsTrino {
		ret[i].IsTrino = isTrino
	}
	for i, forceHttp := range a.ForceHttps {
		ret[i].ForceHttps = forceHttp
	}
	for i, userName := range a.UserName {
		ret[i].UserName = userName
	}
	for i, password := range a.Password {
		ret[i].Password = password
	}
	return ret
}
