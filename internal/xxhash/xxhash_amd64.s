#include "textflag.h"

// XXH64 primes are loaded into registers as needed because IMULQ does
// not accept 64-bit immediates.
//
// Register usage throughout:
//   SI  = data pointer (advances)
//   CX  = bytes remaining
//   BX  = original length n (saved for h += n)
//   AX  = running hash h / final return value
//   DX  = scratch (also receives loaded words)
//   R8  = v1     R9  = v2     R10 = v3     R11 = v4
//   R12 = P1     R13 = P2     R14 = P4     R15 = scratch (P3 / P5)

// func Sum(b []byte) uint64
TEXT ·Sum(SB), NOSPLIT, $0-32
	MOVQ b_base+0(FP), SI
	MOVQ b_len+8(FP), CX
	MOVQ CX, BX                       // n

	MOVQ $11400714785074694791, R12   // P1
	MOVQ $14029467366897019727, R13   // P2
	MOVQ $9650029242287828579, R14    // P4

	CMPQ CX, $32
	JB   small

	// --- big path: 4 lanes of 8-byte rounds over 32-byte stripes ---
	MOVQ $6983438078262162902, R8     // v1 = P1 + P2
	MOVQ $14029467366897019727, R9    // v2 = P2
	XORQ R10, R10                     // v3 = 0
	MOVQ $7046029288634856825, R11    // v4 = -P1

mainloop:
	MOVQ  (SI), DX
	IMULQ R13, DX
	ADDQ  DX, R8
	ROLQ  $31, R8
	IMULQ R12, R8

	MOVQ  8(SI), DX
	IMULQ R13, DX
	ADDQ  DX, R9
	ROLQ  $31, R9
	IMULQ R12, R9

	MOVQ  16(SI), DX
	IMULQ R13, DX
	ADDQ  DX, R10
	ROLQ  $31, R10
	IMULQ R12, R10

	MOVQ  24(SI), DX
	IMULQ R13, DX
	ADDQ  DX, R11
	ROLQ  $31, R11
	IMULQ R12, R11

	ADDQ $32, SI
	SUBQ $32, CX
	CMPQ CX, $32
	JAE  mainloop

	// h = rotl(v1,1) + rotl(v2,7) + rotl(v3,12) + rotl(v4,18)
	MOVQ R8, AX
	ROLQ $1, AX
	MOVQ R9, DX
	ROLQ $7, DX
	ADDQ DX, AX
	MOVQ R10, DX
	ROLQ $12, DX
	ADDQ DX, AX
	MOVQ R11, DX
	ROLQ $18, DX
	ADDQ DX, AX

	// 4× h = (h ^ round(0,vN))*P1 + P4
	// round(0,v) = rotl(v*P2, 31) * P1
	MOVQ  R8, DX
	IMULQ R13, DX
	ROLQ  $31, DX
	IMULQ R12, DX
	XORQ  DX, AX
	IMULQ R12, AX
	ADDQ  R14, AX

	MOVQ  R9, DX
	IMULQ R13, DX
	ROLQ  $31, DX
	IMULQ R12, DX
	XORQ  DX, AX
	IMULQ R12, AX
	ADDQ  R14, AX

	MOVQ  R10, DX
	IMULQ R13, DX
	ROLQ  $31, DX
	IMULQ R12, DX
	XORQ  DX, AX
	IMULQ R12, AX
	ADDQ  R14, AX

	MOVQ  R11, DX
	IMULQ R13, DX
	ROLQ  $31, DX
	IMULQ R12, DX
	XORQ  DX, AX
	IMULQ R12, AX
	ADDQ  R14, AX

	JMP afterbig

small:
	MOVQ $2870177450012600261, AX     // h = P5

afterbig:
	ADDQ BX, AX                        // h += n

	// --- 8-byte tail loop ---
	CMPQ CX, $8
	JB   try4
tail8:
	MOVQ  (SI), DX
	IMULQ R13, DX
	ROLQ  $31, DX
	IMULQ R12, DX                      // round(0, k)
	XORQ  DX, AX
	ROLQ  $27, AX
	IMULQ R12, AX
	ADDQ  R14, AX
	ADDQ  $8, SI
	SUBQ  $8, CX
	CMPQ  CX, $8
	JAE   tail8

try4:
	CMPQ CX, $4
	JB   try1
	MOVL  (SI), DX                     // 32-bit load zero-extends to RDX
	IMULQ R12, DX                      // *P1
	XORQ  DX, AX
	ROLQ  $23, AX
	IMULQ R13, AX                      // *P2
	MOVQ  $1609587929392839161, R15    // P3
	ADDQ  R15, AX
	ADDQ  $4, SI
	SUBQ  $4, CX

try1:
	TESTQ CX, CX
	JZ    avalanche
	MOVQ  $2870177450012600261, R15    // P5
byteloop:
	MOVBQZX (SI), DX
	IMULQ   R15, DX
	XORQ    DX, AX
	ROLQ    $11, AX
	IMULQ   R12, AX
	INCQ    SI
	DECQ    CX
	JNZ     byteloop

avalanche:
	MOVQ  AX, DX
	SHRQ  $33, DX
	XORQ  DX, AX
	IMULQ R13, AX                      // *P2
	MOVQ  AX, DX
	SHRQ  $29, DX
	XORQ  DX, AX
	MOVQ  $1609587929392839161, R15    // P3
	IMULQ R15, AX
	MOVQ  AX, DX
	SHRQ  $32, DX
	XORQ  DX, AX

	MOVQ AX, ret+24(FP)
	RET
