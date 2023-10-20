// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: penumbra/custody/v1alpha1/custody.proto

package custodyv1alpha1

import (
	context "context"
	fmt "fmt"
	grpc1 "github.com/cosmos/gogoproto/grpc"
	proto "github.com/cosmos/gogoproto/proto"
	v1alpha11 "github.com/strangelove-ventures/interchaintest/v8/chain/penumbra/core/keys/v1alpha1"
	v1alpha1 "github.com/strangelove-ventures/interchaintest/v8/chain/penumbra/core/transaction/v1alpha1"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type AuthorizeRequest struct {
	// The transaction plan to authorize.
	Plan *v1alpha1.TransactionPlan `protobuf:"bytes,1,opt,name=plan,proto3" json:"plan,omitempty"`
	// Identifies the FVK (and hence the spend authorization key) to use for signing.
	WalletId *v1alpha11.WalletId `protobuf:"bytes,2,opt,name=wallet_id,json=walletId,proto3" json:"wallet_id,omitempty"`
	// Optionally, pre-authorization data, if required by the custodian.
	//
	// Multiple `PreAuthorization` packets can be included in a single request,
	// to support multi-party pre-authorizations.
	PreAuthorizations []*PreAuthorization `protobuf:"bytes,3,rep,name=pre_authorizations,json=preAuthorizations,proto3" json:"pre_authorizations,omitempty"`
}

func (m *AuthorizeRequest) Reset()         { *m = AuthorizeRequest{} }
func (m *AuthorizeRequest) String() string { return proto.CompactTextString(m) }
func (*AuthorizeRequest) ProtoMessage()    {}
func (*AuthorizeRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8c8c99775232419d, []int{0}
}
func (m *AuthorizeRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *AuthorizeRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_AuthorizeRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *AuthorizeRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AuthorizeRequest.Merge(m, src)
}
func (m *AuthorizeRequest) XXX_Size() int {
	return m.Size()
}
func (m *AuthorizeRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_AuthorizeRequest.DiscardUnknown(m)
}

var xxx_messageInfo_AuthorizeRequest proto.InternalMessageInfo

func (m *AuthorizeRequest) GetPlan() *v1alpha1.TransactionPlan {
	if m != nil {
		return m.Plan
	}
	return nil
}

func (m *AuthorizeRequest) GetWalletId() *v1alpha11.WalletId {
	if m != nil {
		return m.WalletId
	}
	return nil
}

func (m *AuthorizeRequest) GetPreAuthorizations() []*PreAuthorization {
	if m != nil {
		return m.PreAuthorizations
	}
	return nil
}

type AuthorizeResponse struct {
	Data *v1alpha1.AuthorizationData `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *AuthorizeResponse) Reset()         { *m = AuthorizeResponse{} }
func (m *AuthorizeResponse) String() string { return proto.CompactTextString(m) }
func (*AuthorizeResponse) ProtoMessage()    {}
func (*AuthorizeResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_8c8c99775232419d, []int{1}
}
func (m *AuthorizeResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *AuthorizeResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_AuthorizeResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *AuthorizeResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AuthorizeResponse.Merge(m, src)
}
func (m *AuthorizeResponse) XXX_Size() int {
	return m.Size()
}
func (m *AuthorizeResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_AuthorizeResponse.DiscardUnknown(m)
}

var xxx_messageInfo_AuthorizeResponse proto.InternalMessageInfo

func (m *AuthorizeResponse) GetData() *v1alpha1.AuthorizationData {
	if m != nil {
		return m.Data
	}
	return nil
}

// A pre-authorization packet.  This allows a custodian to delegate (partial)
// signing authority to other authorization mechanisms.  Details of how a
// custodian manages those keys are out-of-scope for the custody protocol and
// are custodian-specific.
type PreAuthorization struct {
	// Types that are valid to be assigned to PreAuthorization:
	//	*PreAuthorization_Ed25519_
	PreAuthorization isPreAuthorization_PreAuthorization `protobuf_oneof:"pre_authorization"`
}

func (m *PreAuthorization) Reset()         { *m = PreAuthorization{} }
func (m *PreAuthorization) String() string { return proto.CompactTextString(m) }
func (*PreAuthorization) ProtoMessage()    {}
func (*PreAuthorization) Descriptor() ([]byte, []int) {
	return fileDescriptor_8c8c99775232419d, []int{2}
}
func (m *PreAuthorization) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PreAuthorization) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PreAuthorization.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PreAuthorization) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PreAuthorization.Merge(m, src)
}
func (m *PreAuthorization) XXX_Size() int {
	return m.Size()
}
func (m *PreAuthorization) XXX_DiscardUnknown() {
	xxx_messageInfo_PreAuthorization.DiscardUnknown(m)
}

var xxx_messageInfo_PreAuthorization proto.InternalMessageInfo

type isPreAuthorization_PreAuthorization interface {
	isPreAuthorization_PreAuthorization()
	MarshalTo([]byte) (int, error)
	Size() int
}

type PreAuthorization_Ed25519_ struct {
	Ed25519 *PreAuthorization_Ed25519 `protobuf:"bytes,1,opt,name=ed25519,proto3,oneof" json:"ed25519,omitempty"`
}

func (*PreAuthorization_Ed25519_) isPreAuthorization_PreAuthorization() {}

func (m *PreAuthorization) GetPreAuthorization() isPreAuthorization_PreAuthorization {
	if m != nil {
		return m.PreAuthorization
	}
	return nil
}

func (m *PreAuthorization) GetEd25519() *PreAuthorization_Ed25519 {
	if x, ok := m.GetPreAuthorization().(*PreAuthorization_Ed25519_); ok {
		return x.Ed25519
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*PreAuthorization) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*PreAuthorization_Ed25519_)(nil),
	}
}

// An Ed25519-based preauthorization, containing an Ed25519 signature over the
// `TransactionPlan`.
type PreAuthorization_Ed25519 struct {
	// The Ed25519 verification key used to verify the signature.
	Vk []byte `protobuf:"bytes,1,opt,name=vk,proto3" json:"vk,omitempty"`
	// The Ed25519 signature over the `TransactionPlan`.
	Sig []byte `protobuf:"bytes,2,opt,name=sig,proto3" json:"sig,omitempty"`
}

func (m *PreAuthorization_Ed25519) Reset()         { *m = PreAuthorization_Ed25519{} }
func (m *PreAuthorization_Ed25519) String() string { return proto.CompactTextString(m) }
func (*PreAuthorization_Ed25519) ProtoMessage()    {}
func (*PreAuthorization_Ed25519) Descriptor() ([]byte, []int) {
	return fileDescriptor_8c8c99775232419d, []int{2, 0}
}
func (m *PreAuthorization_Ed25519) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PreAuthorization_Ed25519) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PreAuthorization_Ed25519.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PreAuthorization_Ed25519) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PreAuthorization_Ed25519.Merge(m, src)
}
func (m *PreAuthorization_Ed25519) XXX_Size() int {
	return m.Size()
}
func (m *PreAuthorization_Ed25519) XXX_DiscardUnknown() {
	xxx_messageInfo_PreAuthorization_Ed25519.DiscardUnknown(m)
}

var xxx_messageInfo_PreAuthorization_Ed25519 proto.InternalMessageInfo

func (m *PreAuthorization_Ed25519) GetVk() []byte {
	if m != nil {
		return m.Vk
	}
	return nil
}

func (m *PreAuthorization_Ed25519) GetSig() []byte {
	if m != nil {
		return m.Sig
	}
	return nil
}

func init() {
	proto.RegisterType((*AuthorizeRequest)(nil), "penumbra.custody.v1alpha1.AuthorizeRequest")
	proto.RegisterType((*AuthorizeResponse)(nil), "penumbra.custody.v1alpha1.AuthorizeResponse")
	proto.RegisterType((*PreAuthorization)(nil), "penumbra.custody.v1alpha1.PreAuthorization")
	proto.RegisterType((*PreAuthorization_Ed25519)(nil), "penumbra.custody.v1alpha1.PreAuthorization.Ed25519")
}

func init() {
	proto.RegisterFile("penumbra/custody/v1alpha1/custody.proto", fileDescriptor_8c8c99775232419d)
}

var fileDescriptor_8c8c99775232419d = []byte{
	// 538 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x93, 0x4f, 0x6b, 0x13, 0x41,
	0x18, 0xc6, 0xb3, 0x9b, 0x62, 0xed, 0xb4, 0x48, 0x32, 0x82, 0xa4, 0x11, 0x97, 0x12, 0xa8, 0x16,
	0xaa, 0xb3, 0x24, 0x35, 0xa0, 0xf1, 0xd4, 0x54, 0xa9, 0x3d, 0x88, 0xcb, 0x2a, 0x2a, 0x25, 0x58,
	0x26, 0x9b, 0xd7, 0x64, 0xc9, 0x66, 0x67, 0x9d, 0x99, 0xdd, 0x52, 0x4f, 0x7e, 0x04, 0x3f, 0x83,
	0xe0, 0xc5, 0x83, 0x9f, 0x43, 0x3c, 0xf5, 0xe8, 0x51, 0x92, 0x9b, 0x5f, 0xc1, 0x8b, 0xec, 0x9f,
	0xe9, 0x6e, 0xa3, 0xd1, 0x9e, 0xb2, 0xf3, 0xbc, 0xcf, 0xfc, 0xe6, 0x7d, 0x9f, 0xcc, 0xa0, 0x5b,
	0x01, 0xf8, 0xe1, 0xa4, 0xcf, 0xa9, 0xe9, 0x84, 0x42, 0xb2, 0xc1, 0x89, 0x19, 0x35, 0xa9, 0x17,
	0x8c, 0x68, 0x53, 0x09, 0x24, 0xe0, 0x4c, 0x32, 0xbc, 0xae, 0x8c, 0x44, 0xe9, 0xca, 0x58, 0xbf,
	0x99, 0x33, 0x18, 0x07, 0x73, 0x0c, 0x27, 0x22, 0xa7, 0xc4, 0xab, 0x14, 0x51, 0xbf, 0x7b, 0xde,
	0x27, 0x39, 0xf5, 0x05, 0x75, 0xa4, 0xcb, 0xfc, 0xdc, 0x5e, 0x10, 0xd3, 0x5d, 0x8d, 0x5f, 0x1a,
	0xaa, 0xec, 0x86, 0x72, 0xc4, 0xb8, 0xfb, 0x0e, 0x6c, 0x78, 0x1b, 0x82, 0x90, 0x78, 0x1f, 0x2d,
	0x05, 0x1e, 0xf5, 0x6b, 0xda, 0x86, 0xb6, 0xb5, 0xda, 0xda, 0x21, 0x79, 0x73, 0x8c, 0x03, 0x29,
	0x42, 0x14, 0x99, 0x3c, 0xcf, 0x45, 0xcb, 0xa3, 0xbe, 0x9d, 0x00, 0x70, 0x17, 0xad, 0x1c, 0x53,
	0xcf, 0x03, 0x79, 0xe4, 0x0e, 0x6a, 0x7a, 0x42, 0xdb, 0x9c, 0xa3, 0x25, 0x13, 0x9c, 0x61, 0x5e,
	0x26, 0xee, 0x83, 0x81, 0x7d, 0xf9, 0x38, 0xfb, 0xc2, 0x87, 0x08, 0x07, 0x1c, 0x8e, 0x68, 0xd6,
	0x24, 0x8d, 0x8f, 0x10, 0xb5, 0xf2, 0x46, 0x79, 0x6b, 0xb5, 0xb5, 0x4d, 0x16, 0xe6, 0x46, 0x2c,
	0x0e, 0xbb, 0xc5, 0x3d, 0x76, 0x35, 0x98, 0x53, 0x44, 0xe3, 0x35, 0xaa, 0x16, 0x86, 0x17, 0x01,
	0xf3, 0x05, 0xe0, 0x03, 0xb4, 0x34, 0xa0, 0x92, 0x66, 0xd3, 0xb7, 0x2f, 0x32, 0xfd, 0x39, 0xec,
	0x43, 0x2a, 0xa9, 0x9d, 0x20, 0x1a, 0x9f, 0x34, 0x54, 0x99, 0xef, 0x03, 0x3f, 0x45, 0xcb, 0x30,
	0x68, 0xb5, 0xdb, 0xcd, 0xfb, 0x7f, 0x09, 0xf8, 0x7f, 0x53, 0x90, 0x47, 0xe9, 0xd6, 0xc7, 0x25,
	0x5b, 0x51, 0xea, 0xdb, 0x68, 0x39, 0x53, 0xf1, 0x15, 0xa4, 0x47, 0xe3, 0x04, 0xbb, 0x66, 0xeb,
	0xd1, 0x18, 0x57, 0x50, 0x59, 0xb8, 0xc3, 0x24, 0xfa, 0x35, 0x3b, 0xfe, 0xec, 0x5e, 0x45, 0xd5,
	0x3f, 0xe2, 0x6c, 0xbd, 0xd7, 0xd0, 0xb5, 0xbd, 0xf4, 0x68, 0x2b, 0xbe, 0x16, 0x0e, 0xf3, 0x9e,
	0x01, 0x8f, 0x5c, 0x07, 0xf0, 0x1b, 0xb4, 0x72, 0x16, 0x11, 0xfe, 0x57, 0xde, 0xf3, 0xb7, 0xa8,
	0x7e, 0xfb, 0x62, 0xe6, 0x34, 0xf5, 0xee, 0x17, 0xfd, 0xeb, 0xd4, 0xd0, 0x4e, 0xa7, 0x86, 0xf6,
	0x63, 0x6a, 0x68, 0x1f, 0x66, 0x46, 0xe9, 0x74, 0x66, 0x94, 0xbe, 0xcf, 0x8c, 0x12, 0xba, 0xe1,
	0xb0, 0xc9, 0x62, 0x56, 0x77, 0xad, 0xd8, 0xb9, 0xa5, 0x1d, 0xd2, 0xa1, 0x2b, 0x47, 0x61, 0x9f,
	0x38, 0x6c, 0x62, 0x8a, 0xf8, 0xef, 0x1a, 0x82, 0xc7, 0x22, 0xb8, 0x13, 0x81, 0x2f, 0x43, 0x0e,
	0xc2, 0x74, 0x7d, 0x09, 0xdc, 0x19, 0xd1, 0xf8, 0x57, 0x48, 0x33, 0xba, 0x67, 0x26, 0x0b, 0x73,
	0xe1, 0x63, 0x7d, 0x90, 0x09, 0x6a, 0xfd, 0x51, 0x2f, 0x5b, 0x7b, 0xaf, 0x3e, 0xeb, 0xeb, 0x96,
	0x6a, 0x2a, 0x6b, 0x81, 0xbc, 0xc8, 0x1c, 0xdf, 0xf2, 0x5a, 0x2f, 0xab, 0xf5, 0x54, 0x6d, 0xaa,
	0x6f, 0x2e, 0xac, 0xf5, 0xf6, 0xad, 0xee, 0x13, 0x90, 0x34, 0xbe, 0x3d, 0x3f, 0xf5, 0xeb, 0xca,
	0xd7, 0xe9, 0x64, 0xc6, 0x4e, 0x47, 0x39, 0xfb, 0x97, 0x92, 0x07, 0xbc, 0xf3, 0x3b, 0x00, 0x00,
	0xff, 0xff, 0xbe, 0xd5, 0xb0, 0x6e, 0x64, 0x04, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// CustodyProtocolServiceClient is the client API for CustodyProtocolService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type CustodyProtocolServiceClient interface {
	// Requests authorization of the transaction with the given description.
	Authorize(ctx context.Context, in *AuthorizeRequest, opts ...grpc.CallOption) (*AuthorizeResponse, error)
}

type custodyProtocolServiceClient struct {
	cc grpc1.ClientConn
}

func NewCustodyProtocolServiceClient(cc grpc1.ClientConn) CustodyProtocolServiceClient {
	return &custodyProtocolServiceClient{cc}
}

func (c *custodyProtocolServiceClient) Authorize(ctx context.Context, in *AuthorizeRequest, opts ...grpc.CallOption) (*AuthorizeResponse, error) {
	out := new(AuthorizeResponse)
	err := c.cc.Invoke(ctx, "/penumbra.custody.v1alpha1.CustodyProtocolService/Authorize", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CustodyProtocolServiceServer is the server API for CustodyProtocolService service.
type CustodyProtocolServiceServer interface {
	// Requests authorization of the transaction with the given description.
	Authorize(context.Context, *AuthorizeRequest) (*AuthorizeResponse, error)
}

// UnimplementedCustodyProtocolServiceServer can be embedded to have forward compatible implementations.
type UnimplementedCustodyProtocolServiceServer struct {
}

func (*UnimplementedCustodyProtocolServiceServer) Authorize(ctx context.Context, req *AuthorizeRequest) (*AuthorizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Authorize not implemented")
}

func RegisterCustodyProtocolServiceServer(s grpc1.Server, srv CustodyProtocolServiceServer) {
	s.RegisterService(&_CustodyProtocolService_serviceDesc, srv)
}

func _CustodyProtocolService_Authorize_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthorizeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CustodyProtocolServiceServer).Authorize(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/penumbra.custody.v1alpha1.CustodyProtocolService/Authorize",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CustodyProtocolServiceServer).Authorize(ctx, req.(*AuthorizeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _CustodyProtocolService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "penumbra.custody.v1alpha1.CustodyProtocolService",
	HandlerType: (*CustodyProtocolServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Authorize",
			Handler:    _CustodyProtocolService_Authorize_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "penumbra/custody/v1alpha1/custody.proto",
}

func (m *AuthorizeRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *AuthorizeRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *AuthorizeRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.PreAuthorizations) > 0 {
		for iNdEx := len(m.PreAuthorizations) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.PreAuthorizations[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintCustody(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if m.WalletId != nil {
		{
			size, err := m.WalletId.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintCustody(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if m.Plan != nil {
		{
			size, err := m.Plan.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintCustody(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *AuthorizeResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *AuthorizeResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *AuthorizeResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Data != nil {
		{
			size, err := m.Data.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintCustody(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *PreAuthorization) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PreAuthorization) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PreAuthorization) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.PreAuthorization != nil {
		{
			size := m.PreAuthorization.Size()
			i -= size
			if _, err := m.PreAuthorization.MarshalTo(dAtA[i:]); err != nil {
				return 0, err
			}
		}
	}
	return len(dAtA) - i, nil
}

func (m *PreAuthorization_Ed25519_) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PreAuthorization_Ed25519_) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.Ed25519 != nil {
		{
			size, err := m.Ed25519.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintCustody(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}
func (m *PreAuthorization_Ed25519) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PreAuthorization_Ed25519) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PreAuthorization_Ed25519) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Sig) > 0 {
		i -= len(m.Sig)
		copy(dAtA[i:], m.Sig)
		i = encodeVarintCustody(dAtA, i, uint64(len(m.Sig)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Vk) > 0 {
		i -= len(m.Vk)
		copy(dAtA[i:], m.Vk)
		i = encodeVarintCustody(dAtA, i, uint64(len(m.Vk)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintCustody(dAtA []byte, offset int, v uint64) int {
	offset -= sovCustody(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *AuthorizeRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Plan != nil {
		l = m.Plan.Size()
		n += 1 + l + sovCustody(uint64(l))
	}
	if m.WalletId != nil {
		l = m.WalletId.Size()
		n += 1 + l + sovCustody(uint64(l))
	}
	if len(m.PreAuthorizations) > 0 {
		for _, e := range m.PreAuthorizations {
			l = e.Size()
			n += 1 + l + sovCustody(uint64(l))
		}
	}
	return n
}

func (m *AuthorizeResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Data != nil {
		l = m.Data.Size()
		n += 1 + l + sovCustody(uint64(l))
	}
	return n
}

func (m *PreAuthorization) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.PreAuthorization != nil {
		n += m.PreAuthorization.Size()
	}
	return n
}

func (m *PreAuthorization_Ed25519_) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Ed25519 != nil {
		l = m.Ed25519.Size()
		n += 1 + l + sovCustody(uint64(l))
	}
	return n
}
func (m *PreAuthorization_Ed25519) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Vk)
	if l > 0 {
		n += 1 + l + sovCustody(uint64(l))
	}
	l = len(m.Sig)
	if l > 0 {
		n += 1 + l + sovCustody(uint64(l))
	}
	return n
}

func sovCustody(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozCustody(x uint64) (n int) {
	return sovCustody(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *AuthorizeRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowCustody
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: AuthorizeRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AuthorizeRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Plan", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Plan == nil {
				m.Plan = &v1alpha1.TransactionPlan{}
			}
			if err := m.Plan.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field WalletId", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.WalletId == nil {
				m.WalletId = &v1alpha11.WalletId{}
			}
			if err := m.WalletId.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field PreAuthorizations", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.PreAuthorizations = append(m.PreAuthorizations, &PreAuthorization{})
			if err := m.PreAuthorizations[len(m.PreAuthorizations)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipCustody(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthCustody
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *AuthorizeResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowCustody
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: AuthorizeResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AuthorizeResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Data == nil {
				m.Data = &v1alpha1.AuthorizationData{}
			}
			if err := m.Data.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipCustody(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthCustody
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PreAuthorization) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowCustody
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PreAuthorization: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PreAuthorization: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Ed25519", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &PreAuthorization_Ed25519{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.PreAuthorization = &PreAuthorization_Ed25519_{v}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipCustody(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthCustody
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PreAuthorization_Ed25519) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowCustody
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Ed25519: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Ed25519: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Vk", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Vk = append(m.Vk[:0], dAtA[iNdEx:postIndex]...)
			if m.Vk == nil {
				m.Vk = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Sig", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthCustody
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthCustody
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Sig = append(m.Sig[:0], dAtA[iNdEx:postIndex]...)
			if m.Sig == nil {
				m.Sig = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipCustody(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthCustody
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipCustody(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowCustody
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowCustody
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthCustody
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupCustody
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthCustody
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthCustody        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowCustody          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupCustody = fmt.Errorf("proto: unexpected end of group")
)
