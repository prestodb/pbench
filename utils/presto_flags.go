package utils

import (
	"github.com/spf13/cobra"
	"os"
	"pbench/presto"
)

const DefaultServerUrl = "http://127.0.0.1:8080"

type NewPrestoClientFn func() *presto.Client

type PrestoFlags struct {
	ServerUrl  string
	OutputPath string
	IsTrino    bool
	ForceHttps bool
	UserName   string
	Password   string
}

func (pf *PrestoFlags) InstallPrestoFlags(cmd *cobra.Command) {
	wd, _ := os.Getwd()
	cmd.Flags().StringVarP(&pf.ServerUrl, "server", "s", DefaultServerUrl, "Presto server address")
	cmd.Flags().StringVarP(&pf.OutputPath, "output-path", "o", wd, "Output directory path")
	cmd.Flags().BoolVarP(&pf.IsTrino, "trino", "", false, "Use Trino protocol")
	cmd.Flags().BoolVarP(&pf.ForceHttps, "force-https", "", false, "Force all API requests to use HTTPS")
	cmd.Flags().StringVarP(&pf.UserName, "user", "u", presto.DefaultUser, "Presto user name")
	cmd.Flags().StringVarP(&pf.Password, "password", "p", "", "Presto user password (optional)")
}

func (pf *PrestoFlags) NewPrestoClient() *presto.Client {
	client, _ := presto.NewClient(pf.ServerUrl, pf.IsTrino)
	if pf.UserName != "" {
		if pf.Password != "" {
			client.UserPassword(pf.UserName, pf.Password)
		} else {
			client.User(pf.UserName)
		}
	}
	if pf.ForceHttps {
		client.ForceHttps()
	}
	return client
}
