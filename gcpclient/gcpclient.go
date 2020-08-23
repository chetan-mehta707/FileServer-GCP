package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"cloud.google.com/go/storage"
)

type storageConnection struct {
	Client *storage.Client
}

var (
	client *storageConnection
	once   sync.Once
)

const (
	// GCSBucket name
	GCSBucket = "GCSBucketName"
	// ProjectID Google Project ID name
	ProjectID = "YOUR_PROJECT_ID"
	delimitor = "_"
)

// GetGCSClient gets singleton object for Google Storage
func GetGCSClient(ctx context.Context) (*storage.Client, error) {
	var clientErr error
	once.Do(func() {
		storageClient, err := storage.NewClient(ctx)
		if err != nil {
			clientErr = fmt.Errorf("Failed to create GCS client ERROR:%s", err.Error())
		} else {
			client = &storageConnection{
				Client: storageClient,
			}
		}
	})
	return client.Client, clientErr
}

// Upload API will take Multi Form data as an input and store the object to Google storag
func Upload(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	err := r.ParseMultipartForm(100 << 20) // Max Size Limit is 100 MB
	if err != nil {
		fmt.Println("Error ", err.Error())
		return
	}
	// fileKey is the name of key passed with mutli form request
	fhs := r.MultipartForm.File["fileKey"]
	// Multiple files can be passed as part of Multi form request
	var fileLinks []string
	for _, fh := range fhs {
		link, err := UploadFileToGCSBucket(ctx, fh)
		if err != nil {
			fmt.Println("Error ", err.Error())
			return
		}
		fileLinks = append(fileLinks, link)
	}
}

// UploadFileToGCSBucket will create a date wise directory bucket
func UploadFileToGCSBucket(ctx context.Context, fh *multipart.FileHeader) (string, error) {

	date := time.Now()
	dateStr := date.Format("01-02-2006") // MM-DD-YYYY Format

	file, err := fh.Open()
	if err != nil {
		return "", err
	}
	defer file.Close()

	filename := generateFileNameForGCS(ctx, fh.Filename)
	filepath := fmt.Sprintf("%s/%s%s%s", dateStr, filename, delimitor, fh.Filename)
	client, err := GetGCSClient(ctx)
	if err != nil {
		return "", err
	}
	wc := client.Bucket(GCSBucket).UserProject(ProjectID).Object(filepath).NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		return "", err
	}
	if err := wc.Close(); err != nil {
		return "", err
	}
	return filepath, nil
}

// generateFileNameForGCS will generate the resource path for a file.
// It will use a combination of current time and filename to generate a unique entry.
func generateFileNameForGCS(ctx context.Context, name string) string {
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

// Download gets a file from GCS bucket, Takes file path as a path param from request
func Download(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Caling Download")
	clientCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := mux.Vars(r)["dir"]
	filename := mux.Vars(r)["filename"]
	filePath := fmt.Sprintf("%s/%s", dir, filename)
	client, err := GetGCSClient(clientCtx)
	reader, err := client.Bucket(GCSBucket).UserProject(ProjectID).Object(filePath).NewReader(clientCtx)
	if err != nil {
		fmt.Println("Error ", err.Error())
		return
	}
	defer reader.Close()
	contentType := reader.ContentType()
	size := strconv.FormatInt(reader.Size(), 10)
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		fmt.Println("Error ", err.Error())
		return
	}
	w.Header().Set("Content-Type", contentType)
	disposition := "attachment"
	if strings.Contains(contentType, "image") || strings.Contains(contentType, "pdf") {
		disposition = "inline"
	}
	// The header field Content-Disposition helps the client(Browser)
	// to decide whether the content can be rendered on the browser or downloaded.
	w.Header().Set("Content-Disposition", disposition+"; filename="+filename)
	w.Header().Set("Content-Length", size)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.WriteHeader(http.StatusOK)
	writer := gzip.NewWriter(w)
	defer writer.Close()
	writer.Write(content)
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/upload", Upload).Methods("POST")
	router.HandleFunc("/file/{dir}/{filename}", Download).Methods("GET")
	http.ListenAndServe(":8081", router)
}
