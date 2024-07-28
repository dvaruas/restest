package utils

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func RetryFunc(
	timeout time.Duration,
	fn func() error,
) error {
	var err error
	deadline := time.Now().Add(timeout)
	// Initial time to sleep between tries.
	pause := 50 * time.Millisecond
	// Cutoff for exponential backoff.
	maxPause := 1 * time.Second
	for tryCount := 0; time.Until(deadline) >= 0; {
		if err = fn(); err == nil {
			return nil
		}

		time.Sleep(pause)
		pause = 2 * pause
		if pause > maxPause {
			pause = maxPause
		}
		tryCount++
		fmt.Printf("RetryFunc: try [%v], error: %v\n", tryCount, err)
	}
	return err
}

func DownloadURLToPath(
	ctx context.Context,
	filePath string,
	downloadURL string,
) error {
	out, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("DownloadURLToPath: %w", err)
	}
	defer out.Close()

	statusCode, respBody, err := DoHTTPStreamedCommunication(
		ctx,
		downloadURL,
		http.MethodGet,
		http.NoBody,
		nil)
	if err != nil {
		return fmt.Errorf("DownloadURLToPath: %w", err)
	}

	defer respBody.Close()

	if statusCode != http.StatusOK {
		responseBytes, err := io.ReadAll(respBody)
		if err != nil {
			return fmt.Errorf("DownloadURLToPath: %w", err)
		}
		return fmt.Errorf("DownloadURLToPath: failed with (status: %v, response: %v)", statusCode, string(responseBytes))
	}

	_, err = io.Copy(out, respBody)
	if err != nil {
		return fmt.Errorf("DownloadURLToPath: failed to stream content from download URL to file: %w", err)
	}

	return nil
}

// Uses Get to download and PUT to upload
func DownloadAndUpload(
	ctx context.Context,
	downloadURL string,
	downloadHeaders map[string]string,
	uploadURL string,
	uploadHeaders map[string]string,
) error {
	if downloadURL == "" || uploadURL == "" {
		// nothing to do
		return nil
	}

	statusCode, downloadRespBody, err := DoHTTPStreamedCommunication(
		ctx,
		downloadURL,
		http.MethodGet,
		http.NoBody,
		downloadHeaders)
	if err != nil {
		return fmt.Errorf("DownloadAndUpload: %w", err)
	}

	defer downloadRespBody.Close()

	if statusCode != http.StatusOK {
		responseBytes, err := io.ReadAll(downloadRespBody)
		if err != nil {
			return fmt.Errorf("DownloadAndUpload: %w", err)
		}
		return fmt.Errorf("DownloadAndUpload: failed with (status: %v, response: %v)", statusCode, string(responseBytes))
	}

	statusCode, uploadRespBody, err := DoHTTPStreamedCommunication(
		ctx,
		uploadURL,
		http.MethodPut,
		downloadRespBody,
		uploadHeaders)
	if err != nil {
		return fmt.Errorf("DownloadAndUpload: %w", err)
	}

	defer uploadRespBody.Close()

	if statusCode != http.StatusOK {
		responseBytes, err := io.ReadAll(uploadRespBody)
		if err != nil {
			return fmt.Errorf("DownloadAndUpload: %w", err)
		}
		return fmt.Errorf("DownloadAndUpload: failed with (status: %v, response: %v)", statusCode, string(responseBytes))
	}

	fmt.Printf("DownloadAndUpload stats === \ndownloaded from - %v\nuploaded to - %v\n\n", downloadURL, uploadURL)

	return nil
}

func ExtractPayloadFromZip(
	zipfilePath string,
	filename string,
) ([]byte, error) {
	zr, err := zip.OpenReader(zipfilePath)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	for _, f := range zr.File {
		if f.Name != filename {
			continue
		}
		zf, ferr := f.Open()
		if ferr != nil {
			return nil, ferr
		}
		defer zf.Close()
		return io.ReadAll(zf)
	}
	return nil, fmt.Errorf("file not found in zip")
}

func CaseInsensitiveKeyMapJoin[value any](
	primary, addOn map[string]value,
) map[string]value {
	if primary == nil && addOn == nil {
		return nil
	}

	result := map[string]value{}
	for k, v := range primary {
		result[k] = v
	}
	for k, v := range addOn {
		alreadyExists := false
		for pk := range primary {
			if strings.EqualFold(pk, k) {
				alreadyExists = true
				break
			}
		}
		if alreadyExists {
			continue
		}
		result[k] = v
	}

	return result
}

func PrettyPrintProto(
	m proto.Message,
) string {
	b, err := protojson.Marshal(m)
	if err != nil {
		panic(err)
	}

	var prettyJSON bytes.Buffer
	err = json.Indent(&prettyJSON, b, "", " ")
	if err != nil {
		panic(err)
	}

	return prettyJSON.String()
}
