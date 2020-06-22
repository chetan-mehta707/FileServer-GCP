package gcpclient

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type storageConnection struct {
	Client  *storage.Client
	ConnErr error
}

var (
	client *storageConnection
)

const (
	// GCSBucket name
	GCSBucket = "GCSBucketName"
	delimitor = "_"
)

var once sync.Once

// UploadFileData ...
func UploadFileData(ctx context.Context, log *log.Logger, w http.ResponseWriter, r *http.Request, params map[string]string) error {
	err := r.ParseMultipartForm(100 << 20) // Max Size Limit is 100 MB
	if err != nil {
		return nil
	}
	fhs := r.MultipartForm.File["FileUploadKey"]
	var links []string
	for _, fh := range fhs {
		link, err := UploadFileToGCSBucket(ctx, log, fh)
		if err != nil {
			return nil
		}
		links = append(links, link)
	}
	// response := testrun.FileUploadResponse{Status: testrun.OK, Message: "Files Uploaded Successfully", Links: links}
	// web.Respond(ctx, log, w, response, http.StatusOK)
	return nil
}

// DownloadFileFromGCS gets a file from GCS bucket
func DownloadFileFromGCS(ctx context.Context, log *log.Logger, w http.ResponseWriter, r *http.Request, params map[string]string) error {
	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir := params["dir"]
	filename := params["filename"]
	filePath := fmt.Sprintf("%s/%s", dir, filename)
	client, err := GetGCSClient(clientCtx, log)
	reader, err := client.Bucket(GCSBucket).Object(filePath).NewReader(clientCtx)
	if err != nil {
		return nil
	}
	defer reader.Close()
	contentType := reader.ContentType()
	size := strconv.FormatInt(reader.Size(), 10)
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil
	}
	w.Header().Set("Content-Type", contentType)
	disposition := "attachment"
	if strings.Contains(contentType, "image") || strings.Contains(contentType, "pdf") {
		disposition = "inline"
	}
	w.Header().Set("Content-Disposition", disposition+"; filename="+filename)
	w.Header().Set("Content-Length", size)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.WriteHeader(http.StatusOK)
	writer := gzip.NewWriter(w)
	defer writer.Close()
	writer.Write(content)
	return nil
}

// UploadFileToGCSBucket ...
func UploadFileToGCSBucket(ctx context.Context, log *log.Logger, fh *multipart.FileHeader) (string, error) {

	date := time.Now()
	dateStr := date.Format("01-02-2006") // MM-DD-YYYY Format

	file, err := fh.Open()
	if err != nil {
		return "", err
	}
	defer file.Close()

	filename := generateFileNameForGCS(ctx, log, fh.Filename)
	filepath := fmt.Sprintf("%s/%s%s%s", dateStr, filename, delimitor, fh.Filename)
	client, err := GetGCSClient(ctx, log)
	if err != nil {
		return "", err
	}
	wc := client.Bucket(GCSBucket).Object(filepath).NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		return "", err
	}
	if err := wc.Close(); err != nil {
		return "", err
	}
	return filepath, nil
}

func generateFileNameForGCS(ctx context.Context, log *log.Logger, name string) string {
	time := time.Now().UnixNano()
	var strArr []string
	strArr = append(strArr, name)
	strArr = append(strArr, strconv.Itoa(int(time)))
	var filename string
	for _, str := range strArr {
		filename = filename + str
	}
	return filename
}

// GetGCSClient singleton object ...
func GetGCSClient(ctx context.Context, logger *log.Logger) (*storage.Client, error) {
	var clientErr error
	authKey := "" // JSON auth Key for accessing GCP resource
	once.Do(func() {
		storageClient, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(authKey)))
		if err != nil {
			clientErr = fmt.Errorf("Failed to create GCS client ERROR:%s", err.Error())
		} else {
			client = &storageConnection{
				Client:  storageClient,
				ConnErr: err,
			}
		}
	})
	return client.Client, clientErr
}
