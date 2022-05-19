package cosmos

import (
	"github.com/cosmos/cosmos-sdk/simapp"
	simappparams "github.com/cosmos/cosmos-sdk/simapp/params"
	"github.com/cosmos/cosmos-sdk/std"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	transfertypes "github.com/cosmos/ibc-go/v3/modules/apps/transfer/types"
	ibctypes "github.com/cosmos/ibc-go/v3/modules/core/types"
)

func NewTestEncoding() simappparams.EncodingConfig {
	// core modules
	cfg := simappparams.MakeTestEncodingConfig()
	std.RegisterLegacyAminoCodec(cfg.Amino)
	std.RegisterInterfaces(cfg.InterfaceRegistry)
	simapp.ModuleBasics.RegisterLegacyAminoCodec(cfg.Amino)
	simapp.ModuleBasics.RegisterInterfaces(cfg.InterfaceRegistry)

	// specialized modules
	banktypes.RegisterInterfaces(cfg.InterfaceRegistry)
	ibctypes.RegisterInterfaces(cfg.InterfaceRegistry)
	transfertypes.RegisterInterfaces(cfg.InterfaceRegistry)

	return cfg
}
