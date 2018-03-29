package cmd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/runconduit/conduit/controller/api/util"
	pb "github.com/runconduit/conduit/controller/gen/public"
	"github.com/spf13/cobra"
)

var namespace string
var resource = "all"

// var timeWindow string

var statV2Cmd = &cobra.Command{
	Use:   "statv2 [flags] namespace [NAMESPACE] resource [RESOURCE]",
	Short: "Display runtime statistics about mesh resources",
	Long: `Display runtime statistics about mesh resources.

I should fill this out. `,
	Example: `  I should fill this out `,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println(args)
		switch len(args) {
		case 1:
			namespace = args[0]
		case 2:
			namespace = args[0]
			resource = args[1]
		default:
			namespace = "default"
		}

		client, err := newPublicAPIClient()
		if err != nil {
			return fmt.Errorf("error creating api client while making stats request: %v", err)
		}

		output, err := requestStatsV2FromApi(client)
		if err != nil {
			return err
		}

		_, err = fmt.Print(output)

		return err
	},
}

func init() {
	RootCmd.AddCommand(statV2Cmd)
	addControlPlaneNetworkingArgs(statV2Cmd)
	statV2Cmd.PersistentFlags().StringVarP(&timeWindow, "time-window", "t", "1m", "Stat window.  One of: '10s', '1m', '10m', '1h'.")
}

func requestStatsV2FromApi(client pb.ApiClient) (string, error) {
	req, err := buildMetricRequestV2()

	if err != nil {
		return "", fmt.Errorf("error creating metrics request while making stats request: %v", err)
	}

	resp, err := client.StatV2(context.Background(), req)
	if err != nil {
		return "", fmt.Errorf("error calling stat with request: %v", err)
	}

	return renderStatsV2(resp)
}

func renderStatsV2(resp *pb.MetricResponseV2) (string, error) {
	var buffer bytes.Buffer
	w := tabwriter.NewWriter(&buffer, 0, 0, padding, ' ', tabwriter.AlignRight)
	writeStatsV2ToBuffer(resp, w)
	w.Flush()

	// strip left padding on the first column
	out := string(buffer.Bytes()[padding:])
	out = strings.Replace(out, "\n"+strings.Repeat(" ", padding), "\n", -1)

	return out, nil
}

func writeStatsV2ToBuffer(resp *pb.MetricResponseV2, w *tabwriter.Writer) {
	nameHeader := "NAME"
	maxNameLength := len(nameHeader)

	stats := make(map[string]*row)
	for _, metric := range resp.Metrics {
		if len(metric.Datapoints) == 0 {
			continue
		}

		name := metric.Metadata["k8s_deployment"]

		if len(name) > maxNameLength {
			maxNameLength = len(name)
		}

		if _, ok := stats[name]; !ok {
			stats[name] = &row{}
		}

		switch metric.Name {
		case pb.MetricName_REQUEST_RATE:
			stats[name].requestRate = metric.Datapoints[0].Value.GetGauge()
		case pb.MetricName_SUCCESS_RATE:
			stats[name].successRate = metric.Datapoints[0].Value.GetGauge()
		case pb.MetricName_LATENCY:
			for _, v := range metric.Datapoints[0].Value.GetHistogram().Values {
				switch v.Label {
				case pb.HistogramLabel_P50:
					stats[name].latencyP50 = v.Value
				case pb.HistogramLabel_P99:
					stats[name].latencyP99 = v.Value
				}
			}
		}
	}

	fmt.Fprintln(w, strings.Join([]string{
		nameHeader + strings.Repeat(" ", maxNameLength-len(nameHeader)),
		"REQUEST_RATE",
		"SUCCESS_RATE",
		"P50_LATENCY",
		"P99_LATENCY\t", // trailing \t is required to format last column
	}, "\t"))

	sortedNames := sortStatsKeys(stats)
	for _, name := range sortedNames {
		fmt.Fprintf(
			w,
			"%s\t%.1frps\t%.2f%%\t%dms\t%dms\t\n",
			name+strings.Repeat(" ", maxNameLength-len(name)),
			stats[name].requestRate,
			stats[name].successRate*100,
			stats[name].latencyP50,
			stats[name].latencyP99,
		)
	}
}

func buildMetricRequestV2() (*pb.MetricRequestV2, error) {
	window, err := util.GetWindow(timeWindow)
	if err != nil {
		return nil, err
	}

	return &pb.MetricRequestV2{
		Metrics: []pb.MetricName{
			pb.MetricName_REQUEST_RATE,
		},
		Window:    window,
		Resource:  resource,
		Namespace: namespace,
	}, nil
}
