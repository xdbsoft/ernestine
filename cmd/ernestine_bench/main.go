package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/buger/goterm"
	"github.com/urfave/cli/v2"
	"github.com/xdbsoft/ernestine"
	"github.com/xdbsoft/ernestine/drivers/azblobstorage"
	"github.com/xdbsoft/ernestine/drivers/filesystem"
	"github.com/xdbsoft/ernestine/drivers/memory"
)

type bencher struct {
	count  uint
	client ernestine.Client
}

func main() {

	var b bencher

	app := &cli.App{
		Name:  "ernestine_bench",
		Usage: "cli tool ued to benchmark the drivers of the ernestine framework",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "driver",
				Aliases: []string{"d"},
				Usage:   "the driver to be used",
				Value:   "filesystem",
				Action: func(ctx *cli.Context, driver string) error {
					switch driver {
					case "memory":
						b.client = memory.New()
					case "filesystem":
						var err error
						b.client, err = filesystem.New(ctx.Path("filesystem_path"))
						if err != nil {
							log.Fatal(err)
						}
					case "azblobstorage":
						var err error
						b.client, err = azblobstorage.New(ctx.String("azure_connectionstring"), ctx.String("azure_containername"))
						if err != nil {
							log.Fatal(err)
						}
					default:
						return fmt.Errorf("unknown driver '%s'", driver)
					}
					return nil
				},
			},
			&cli.UintFlag{
				Name:        "count",
				Aliases:     []string{"n"},
				Usage:       "the number of times the operation is to be repeated",
				Value:       1,
				Destination: &b.count,
			},
			&cli.PathFlag{
				Name:    "filesystem_path",
				Aliases: []string{"fs_p"},
				Usage:   "path to the directory where to store blobs on disk for filesystem driver",
			},
			&cli.StringFlag{
				Name:    "azure_connectionstring",
				Aliases: []string{"az_cs"},
				Usage:   "connection string for azure blob storage driver",
			},
			&cli.StringFlag{
				Name:    "azure_containername",
				Aliases: []string{"az_cn"},
				Usage:   "container name for azure blob storage driver",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "create",
				Aliases: []string{"c"},
				Usage:   "create new items",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:        "size",
						Aliases:     []string{"s"},
						Usage:       "The size of the blobs to generate, in bytes",
						Value:       1024,
						DefaultText: "1024 bytes (ie. 1kB)",
					},
				},
				Action: func(ctx *cli.Context) error {

					return b.create(ctx.Context, ctx.Int("size"))
				},
			},
			{
				Name:    "get",
				Aliases: []string{"g"},
				Usage:   "retrieve existing items",
				Action: func(ctx *cli.Context) error {
					return b.get(ctx.Context)
				},
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "list items",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "prefix",
						Aliases: []string{"p"},
						Usage:   "The prefix of the blobs to find",
						Value:   "",
					},
					&cli.IntFlag{
						Name:    "expected",
						Aliases: []string{"x"},
						Usage:   "The expected count of items to find",
						Value:   0,
					},
				},
				Action: func(ctx *cli.Context) error {

					return b.list(ctx.Context, ctx.String("prefix"), ctx.Int("expected"))
				},
			},
			{
				Name:    "delete",
				Aliases: []string{"d"},
				Usage:   "delete existing items",
				Action: func(ctx *cli.Context) error {
					return b.delete(ctx.Context)
				},
			},
			{
				Name:  "cleanup",
				Usage: "cleanup container",
				Action: func(ctx *cli.Context) error {
					return b.cleanup(ctx.Context)
				},
			},
		},
		Suggest: true,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

type durations []time.Duration

func (s durations) Len() int {
	return len(s)
}
func (s durations) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s durations) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (b *bencher) exec(ctx context.Context, f func(client ernestine.Client, idx int) error) error {
	allDurations := make(durations, b.count)
	tStart := time.Now()
	for i := 0; i < int(b.count); i++ {
		tStartItem := time.Now()
		if err := f(b.client, i); err != nil {
			log.Fatal(err)
		}
		allDurations[i] = time.Since(tStartItem)
	}
	dur := time.Since(tStart)
	log.Printf("%d operations in %s (%f op/s, %s/op)", b.count, dur, float64(b.count)/dur.Seconds(), dur/time.Duration(b.count))

	chart := goterm.NewLineChart(140, 10)
	data := new(goterm.DataTable)
	data.AddColumn("Index")
	data.AddColumn("Dur(ms)")
	for i := 0; i < int(b.count); i++ {
		data.AddRow(float64(i+1), float64(allDurations[i].Milliseconds()))
	}
	fmt.Println(chart.Draw(data))

	sort.Sort(allDurations)
	log.Printf("p50=%s p90=%s p99=%s max=%s", allDurations[int(float64(len(allDurations))*0.5)],
		allDurations[int(float64(len(allDurations))*0.9)],
		allDurations[int(float64(len(allDurations))*0.99)],
		allDurations[len(allDurations)-1])

	return nil
}

func (b *bencher) create(ctx context.Context, size int) error {
	return b.exec(ctx, func(c ernestine.Client, idx int) error {
		k := fmt.Sprintf("%020d", idx)

		v := make([]byte, size)
		rand.Read(v)

		return c.Create(k, v)
	})
}

func (b *bencher) get(ctx context.Context) error {
	return b.exec(ctx, func(c ernestine.Client, idx int) error {
		k := fmt.Sprintf("%020d", idx)

		_, err := c.Get(k)
		return err
	})
}

func (b *bencher) list(ctx context.Context, prefix string, expectedCount int) error {
	return b.exec(ctx, func(c ernestine.Client, idx int) error {
		v, err := c.List(prefix)
		if v.Found != expectedCount {
			return fmt.Errorf("unexpected result: got %d expecting %d", v.Found, expectedCount)
		}
		return err
	})
}

func (b *bencher) delete(ctx context.Context) error {
	return b.exec(ctx, func(c ernestine.Client, idx int) error {
		k := fmt.Sprintf("%020d", idx)

		return c.Delete(k)
	})
}

func (b *bencher) cleanup(ctx context.Context) error {
	return b.exec(ctx, func(c ernestine.Client, idx int) error {
		return c.Cleanup()
	})
}
