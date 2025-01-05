package cosmos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/cosmos/cosmos-sdk/types"

	"github.com/strangelove-ventures/interchaintest/v8/testutil"
)

type InstantiateContractAttribute struct {
	Value string `json:"value"`
}

type InstantiateContractEvent struct {
	Attributes []InstantiateContractAttribute `json:"attributes"`
}

type InstantiateContractLog struct {
	Events []InstantiateContractEvent `json:"event"`
}

type InstantiateContractResponse struct {
	Logs []InstantiateContractLog `json:"log"`
}

type QueryContractResponse struct {
	Contracts []string `json:"contracts"`
}

type CodeInfo struct {
	CodeID string `json:"code_id"`
}
type CodeInfosResponse struct {
	CodeInfos []CodeInfo `json:"code_infos"`
}

// StoreContract takes a file path to smart contract and stores it on-chain. Returns the contracts code id.
func (tn *ChainNode) StoreContract(ctx context.Context, keyName string, fileName string, extraExecTxArgs ...string) (string, error) {
	_, file := filepath.Split(fileName)
	err := tn.CopyFile(ctx, fileName, file)
	if err != nil {
		return "", fmt.Errorf("writing contract file to docker volume: %w", err)
	}

	cmd := []string{"wasm", "store", path.Join(tn.HomeDir(), file), "--gas", "auto"}
	cmd = append(cmd, extraExecTxArgs...)

	if _, err := tn.ExecTx(ctx, keyName, cmd...); err != nil {
		return "", err
	}

	err = testutil.WaitForBlocks(ctx, 5, tn.Chain)
	if err != nil {
		return "", fmt.Errorf("wait for blocks: %w", err)
	}

	stdout, _, err := tn.ExecQuery(ctx, "wasm", "list-code", "--reverse")
	if err != nil {
		return "", err
	}

	res := CodeInfosResponse{}
	if err := json.Unmarshal(stdout, &res); err != nil {
		return "", err
	}

	return res.CodeInfos[0].CodeID, nil
}

// InstantiateContract takes a code id for a smart contract and initialization message and returns the instantiated contract address.
func (tn *ChainNode) InstantiateContract(ctx context.Context, keyName string, codeID string, initMessage string, needsNoAdminFlag bool, extraExecTxArgs ...string) (string, error) {
	command := []string{"wasm", "instantiate", codeID, initMessage, "--label", "wasm-contract"}
	command = append(command, extraExecTxArgs...)
	if needsNoAdminFlag {
		command = append(command, "--no-admin")
	}
	txHash, err := tn.ExecTx(ctx, keyName, command...)
	if err != nil {
		return "", err
	}

	txResp, err := tn.GetTransaction(tn.CliContext(), txHash)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction %s: %w", txHash, err)
	}
	if txResp.Code != 0 {
		return "", fmt.Errorf("error in transaction (code: %d): %s", txResp.Code, txResp.RawLog)
	}

	stdout, _, err := tn.ExecQuery(ctx, "wasm", "list-contract-by-code", codeID)
	if err != nil {
		return "", err
	}

	contactsRes := QueryContractResponse{}
	if err := json.Unmarshal(stdout, &contactsRes); err != nil {
		return "", err
	}

	contractAddress := contactsRes.Contracts[len(contactsRes.Contracts)-1]
	return contractAddress, nil
}

// ExecuteContract executes a contract transaction with a message using it's address.
func (tn *ChainNode) ExecuteContract(ctx context.Context, keyName string, contractAddress string, message string, extraExecTxArgs ...string) (res *types.TxResponse, err error) {
	cmd := []string{"wasm", "execute", contractAddress, message}
	cmd = append(cmd, extraExecTxArgs...)

	txHash, err := tn.ExecTx(ctx, keyName, cmd...)
	if err != nil {
		return &types.TxResponse{}, err
	}

	txResp, err := tn.GetTransaction(tn.CliContext(), txHash)
	if err != nil {
		return &types.TxResponse{}, fmt.Errorf("failed to get transaction %s: %w", txHash, err)
	}

	if txResp.Code != 0 {
		return txResp, fmt.Errorf("error in transaction (code: %d): %s", txResp.Code, txResp.RawLog)
	}

	return txResp, nil
}

// QueryContract performs a smart query, taking in a query struct and returning an error with the response struct populated.
func (tn *ChainNode) QueryContract(ctx context.Context, contractAddress string, queryMsg any, response any) error {
	var query []byte
	var err error

	if q, ok := queryMsg.(string); ok {
		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(q), &jsonMap); err != nil {
			return err
		}

		query, err = json.Marshal(jsonMap)
		if err != nil {
			return err
		}
	} else {
		query, err = json.Marshal(queryMsg)
		if err != nil {
			return err
		}
	}

	stdout, _, err := tn.ExecQuery(ctx, "wasm", "contract-state", "smart", contractAddress, string(query))
	if err != nil {
		return err
	}
	err = json.Unmarshal(stdout, response)
	return err
}

// MigrateContract performs contract migration.
func (tn *ChainNode) MigrateContract(ctx context.Context, keyName string, contractAddress string, codeID string, message string, extraExecTxArgs ...string) (res *types.TxResponse, err error) {
	cmd := []string{"wasm", "migrate", contractAddress, codeID, message}
	cmd = append(cmd, extraExecTxArgs...)

	txHash, err := tn.ExecTx(ctx, keyName, cmd...)
	if err != nil {
		return &types.TxResponse{}, err
	}

	txResp, err := tn.GetTransaction(tn.CliContext(), txHash)
	if err != nil {
		return &types.TxResponse{}, fmt.Errorf("failed to get transaction %s: %w", txHash, err)
	}

	if txResp.Code != 0 {
		return txResp, fmt.Errorf("error in transaction (code: %d): %s", txResp.Code, txResp.RawLog)
	}

	return txResp, nil
}

// StoreClientContract takes a file path to a client smart contract and stores it on-chain. Returns the contracts code id.
func (tn *ChainNode) StoreClientContract(ctx context.Context, keyName string, fileName string, extraExecTxArgs ...string) (string, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	_, file := filepath.Split(fileName)
	err = tn.WriteFile(ctx, content, file)
	if err != nil {
		return "", fmt.Errorf("writing contract file to docker volume: %w", err)
	}

	cmd := []string{"ibc-wasm", "store-code", path.Join(tn.HomeDir(), file), "--gas", "auto"}
	cmd = append(cmd, extraExecTxArgs...)

	_, err = tn.ExecTx(ctx, keyName, cmd...)
	if err != nil {
		return "", err
	}

	codeHashByte32 := sha256.Sum256(content)
	codeHash := hex.EncodeToString(codeHashByte32[:])

	return codeHash, nil
}

// DumpContractState dumps the state of a contract at a block height.
func (tn *ChainNode) DumpContractState(ctx context.Context, contractAddress string, height int64) (*DumpContractStateResponse, error) {
	stdout, _, err := tn.ExecQuery(ctx,
		"wasm", "contract-state", "all", contractAddress,
		"--height", fmt.Sprint(height),
	)
	if err != nil {
		return nil, err
	}

	res := new(DumpContractStateResponse)
	if err := json.Unmarshal(stdout, res); err != nil {
		return nil, err
	}
	return res, nil
}

// QueryContractInfo queries the information about a contract like the admin and code_id.
func (tn *ChainNode) QueryContractInfo(ctx context.Context, contractAddress string) (*ContractInfoResponse, error) {
	stdout, _, err := tn.ExecQuery(ctx,
		"wasm", "contract", contractAddress,
	)
	if err != nil {
		return nil, err
	}

	res := new(ContractInfoResponse)
	if err := json.Unmarshal(stdout, res); err != nil {
		return nil, err
	}
	return res, nil
}
