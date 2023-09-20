package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/strangelove-ventures/interchaintest/v8/chain/cosmos"
	"github.com/strangelove-ventures/localinterchain/interchain/util"
)

type upload struct {
	ctx  context.Context
	vals map[string]*cosmos.ChainNode
}

type Uploader struct {
	ChainId  string `json:"chain_id"`
	FilePath string `json:"file_path"`

	// Upload-Type: cosmwasm only
	KeyName string `json:"key_name"`
}

func NewUploader(ctx context.Context, vals map[string]*cosmos.ChainNode) *upload {
	return &upload{
		ctx:  ctx,
		vals: vals,
	}
}

func (u *upload) PostUpload(w http.ResponseWriter, r *http.Request) {
	var upload Uploader
	err := json.NewDecoder(r.Body).Decode(&upload)
	if err != nil {
		util.WriteError(w, err)
		return
	}

	log.Printf("Uploader: %+v", u)

	chainId := upload.ChainId
	if _, ok := u.vals[chainId]; !ok {
		util.Write(w, []byte(fmt.Sprintf(`{"error":"chain_id %s not found"}`, chainId)))
		return
	}

	uploadType := r.Header.Get("Upload-Type")
	if uploadType == "" {
		util.Write(w, []byte(fmt.Sprintf(`{"success":"file uploaded to %s"}`, chainId)))
		return
	}

	if uploadType == "cosmwasm" {
		codeId, err := u.vals[chainId].StoreContract(u.ctx, upload.KeyName, upload.FilePath)
		if err != nil {
			util.WriteError(w, err)
			return
		}

		util.Write(w, []byte(fmt.Sprintf(`{"code_id":%s}`, codeId)))
		return
	}

}
