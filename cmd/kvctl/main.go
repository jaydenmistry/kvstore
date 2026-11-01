// Command kvctl is a command-line client for kvstore.
//
//	kvctl get KEY          print the value; exit status 1 if the key is missing
//	kvctl put KEY VALUE    store VALUE ("-" reads it from standard input)
//	kvctl delete KEY       remove KEY
//	kvctl status           show each server's Raft state
//
// Servers come from -config (the cluster configuration file), -servers, or
// $KVCTL_SERVERS, as comma-separated base URLs, in that order of preference.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jaydenmistry/kvstore/internal/client"
	"github.com/jaydenmistry/kvstore/internal/config"
	"github.com/jaydenmistry/kvstore/internal/server"
)

const defaultServers = "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"

func main() { os.Exit(run()) }

func run() int {
	servers := defaultServers
	if env := os.Getenv("KVCTL_SERVERS"); env != "" {
		servers = env
	}
	flag.StringVar(&servers, "servers", servers, "comma-separated server `URLs`")
	configPath := flag.String("config", "", "cluster configuration `file` to take the servers from")
	timeout := flag.Duration("timeout", 10*time.Second, "give up after this `long`")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl [flags] get KEY | put KEY VALUE | delete KEY | status")
		flag.PrintDefaults()
	}
	flag.Parse()
	urls := strings.Split(servers, ",")
	if *configPath != "" {
		c, err := config.Load(*configPath)
		if err != nil {
			return fail(err)
		}
		urls = c.APIURLs()
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		return 2
	}
	cmd, args := args[0], args[1:]
	if cmd == "status" && len(args) == 0 {
		return status(ctx, urls)
	}
	cl, err := client.New(urls)
	if err != nil {
		return fail(err)
	}
	switch {
	case cmd == "get" && len(args) == 1:
		v, ok, err := cl.Get(ctx, args[0])
		if err != nil {
			return fail(err)
		}
		if !ok {
			fmt.Fprintln(os.Stderr, "kvctl: not found")
			return 1
		}
		os.Stdout.Write(v)
		if isTerminal(os.Stdout) {
			fmt.Println()
		}
	case cmd == "put" && len(args) == 2:
		value := []byte(args[1])
		if args[1] == "-" {
			if value, err = io.ReadAll(os.Stdin); err != nil {
				return fail(err)
			}
		}
		if err := cl.Put(ctx, args[0], value); err != nil {
			return fail(err)
		}
	case cmd == "delete" && len(args) == 1:
		found, err := cl.Delete(ctx, args[0])
		if err != nil {
			return fail(err)
		}
		if !found {
			fmt.Fprintln(os.Stderr, "kvctl: key did not exist")
		}
	default:
		flag.Usage()
		return 2
	}
	return 0
}

func fail(err error) int {
	fmt.Fprintln(os.Stderr, "kvctl:", err)
	return 1
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func status(ctx context.Context, urls []string) int {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SERVER\tID\tSTATE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST\tSNAPSHOT\tSESSIONS")
	for _, u := range urls {
		st, err := fetchStatus(ctx, u)
		if err != nil {
			fmt.Fprintf(w, "%s\tunreachable: %v\n", u, err)
			continue
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			u, st.ID, st.State, st.Term, st.Leader, st.Commit, st.Applied, st.LastIndex, st.Snapshot, st.Sessions)
	}
	w.Flush()
	return 0
}

func fetchStatus(ctx context.Context, base string) (server.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var st server.Status
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/status", nil)
	if err != nil {
		return st, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}
