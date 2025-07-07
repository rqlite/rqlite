package example

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rqlite/rqlite/v8/gcp"
)

func main() {
	ctx := context.Background()

	cfg := gcp.GCSConfig{
		Bucket:          "my-demo-bucket",
		ProjectID:       "my-gcp-project",
		Name:            "sample.txt",
		CredentialsPath: os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
	}

	client, err := gcp.NewGCSClient(&cfg, nil)
	if err != nil {
		panic(err)
	}

	if err := client.EnsureBucket(ctx); err != nil {
		panic(err)
	}
	fmt.Println("bucket ready")

	// ---- upload ------------------------------------------------------------
	payload := strings.NewReader("hello, world\n")
	if err := client.Upload(ctx, payload, "v1"); err != nil {
		panic(err)
	}
	fmt.Println("upload complete")

	// ---- current ID --------------------------------------------------------
	id, err := client.CurrentID(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("current ID: %s\n", id)

	// ---- download ----------------------------------------------------------
	tmp, err := os.CreateTemp("", "gcs-download-*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmp.Name())

	if err := client.Download(ctx, tmp); err != nil {
		panic(err)
	}
	tmp.Seek(0, 0)
	data, _ := io.ReadAll(tmp)
	fmt.Printf("downloaded data: %s", data)

	// ---- delete ------------------------------------------------------------
	if err := client.Delete(ctx); err != nil {
		panic(err)
	}
	fmt.Println("\nobject deleted")
}
