// Command quickstart demonstrates connecting to an indexer, uploading an
// object and downloading it again.
package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	siastorage "go.sia.tech/siastorage"
)

// A persistent, randomly generated ID identifying this application to the
// indexer. Generate your own for a real application.
const appIDHex = "5c0b1af28e6ac76395b2087ea987297b9c496f90d2ab3e3d3d07980ae4c43633"

// connect connects to the indexer using an app key stored at keyPath. If no
// key is stored yet, it walks the user through the connection approval flow
// and stores the resulting key.
func connect(indexerURL, keyPath string) (*siastorage.SDK, error) {
	appID, err := hex.DecodeString(appIDHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode app ID: %w", err)
	}
	builder, err := siastorage.NewBuilder(indexerURL, siastorage.AppMetadata{
		Id:          appID,
		Name:        "Quickstart",
		Description: "Sia Storage SDK quickstart example",
		ServiceUrl:  "https://sia.tech",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create builder: %w", err)
	}

	// try to connect with a previously stored app key
	if keyData, err := os.ReadFile(keyPath); err == nil {
		appKey, err := siastorage.NewAppKey(keyData)
		if err != nil {
			return nil, fmt.Errorf("failed to import app key: %w", err)
		}
		sdk, err := builder.Connected(appKey)
		if err == nil {
			return sdk, nil
		} else if !errors.Is(err, siastorage.ErrNotRegistered) {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
	}

	// otherwise walk through the approval flow
	phrase := siastorage.GenerateRecoveryPhrase()
	fmt.Println("Generated a new recovery phrase - store it securely:")
	fmt.Printf("  %s\n\n", phrase)

	responseURL, err := builder.RequestConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to request connection: %w", err)
	}
	fmt.Println("Approve the connection by visiting:")
	fmt.Printf("  %s\n\n", responseURL)

	if err := builder.WaitForApproval(); err != nil {
		return nil, fmt.Errorf("failed to wait for approval: %w", err)
	}
	sdk, err := builder.Register(phrase)
	if err != nil {
		return nil, fmt.Errorf("failed to register: %w", err)
	}
	if err := os.WriteFile(keyPath, sdk.AppKey().Export(), 0o600); err != nil {
		return nil, fmt.Errorf("failed to store app key: %w", err)
	}
	return sdk, nil
}

func main() {
	indexerURL := flag.String("indexer", "https://sia.storage", "the indexer to connect to")
	keyPath := flag.String("key", "appkey.bin", "path to store the app key at")
	flag.Parse()

	sdk, err := connect(*indexerURL, *keyPath)
	if err != nil {
		log.Fatal(err)
	}

	// upload
	data := []byte("hello from the Sia Storage Go SDK")
	obj, err := sdk.Upload(siastorage.NewObject(), bytes.NewReader(data), siastorage.UploadOptions{})
	if err != nil {
		log.Fatal("failed to upload:", err)
	}
	obj.UpdateMetadata([]byte(`{"name":"hello.txt"}`))
	if err := sdk.PinObject(obj); err != nil {
		log.Fatal("failed to pin object:", err)
	}
	fmt.Println("uploaded object", obj.Id())

	// download
	dl, err := sdk.Download(obj, siastorage.DownloadOptions{})
	if err != nil {
		log.Fatal("failed to start download:", err)
	}
	defer dl.Close()
	downloaded, err := io.ReadAll(dl)
	if err != nil {
		log.Fatal("failed to download:", err)
	}
	if !bytes.Equal(downloaded, data) {
		log.Fatal("downloaded data does not match uploaded data")
	}
	fmt.Printf("downloaded object: %q\n", downloaded)
}
