package xxh3

import (
	"encoding/hex"
	"math/rand"
	"testing"
)

// input is a deterministic n-byte message; the same formula generated the digests below.
func input(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*131 + n*7 + 13)
	}
	return b
}

// vectors are XXH3_128bits (seed 0) digests in canonical (big-endian) form, produced by
// the reference C library, libxxhash 0.8.2 (python xxhash 3.5.0 xxh3_128). The lengths
// cross every code-path boundary: 0, 1-3, 4-8, 9-16, 17-128, 129-240, >240, and the
// 1024-byte block edge.
var vectors = []struct {
	n    int
	want string
}{
	{0, "99aa06d3014798d86001c324468d497f"},
	{1, "f5950428e527e5ba324714f62fca15ce"},
	{2, "6f3b67939f5514f25e7d0c486b62a70d"},
	{3, "d8d8a1a91fc391afe3a0494a0aa2f300"},
	{4, "cc59765079321dce9e8742e7162123fb"},
	{5, "58e065ea5a6704e16b067f69f9c17ac0"},
	{6, "e3a48de55ced523d4f83efacc1949eeb"},
	{7, "a1fccd044a41b48eb1ad435d9849f968"},
	{8, "6314717e39d0d5416b68cf5de2642a76"},
	{9, "578e8464baef0791120a5fac3dc22e5b"},
	{10, "2460c3d11cc8a4615ae99f3f8ae5229b"},
	{11, "0d332fb30ed66348fd39ce78d6badbdc"},
	{12, "ce0ae770d2cc5dfc772d3667d2d4551e"},
	{13, "ade041f8d1bf966911bfbe265e69882a"},
	{14, "1411aa12778294f6d1a5f31a1ffac33d"},
	{15, "b26a6741bb4d4ca0a9314d13821d75f7"},
	{16, "28fdf65cbab48cfd153862a66fb80a33"},
	{17, "7841d0aea9cf9e1d526115262da7e9b6"},
	{18, "e7189827ceb2e5d064d8988b90ff298a"},
	{19, "790945a2b85ecccd5695ee6e3b14f90d"},
	{31, "4eaf2e314b7a15e7f83c53faf012740a"},
	{32, "767cefd8c7906d3ca8273d544e66a19c"},
	{33, "9e1f1764c144faee5bbec434d4ede54e"},
	{63, "3e7d05fc39d70622e7b52252ee77cefd"},
	{64, "63ef345bee5a14192e05dd4eb44090a6"},
	{65, "0446fa30244aa140eabb2bf4c0c8af98"},
	{96, "fb46421a1e19c3390485a86f2ab3897b"},
	{127, "2ea7717f118fe1addc73d4b46c21870f"},
	{128, "02c71466e309eea8f14326013b278183"},
	{129, "42d8dac5b3a4b7f221fbdf124ddd954a"},
	{130, "5128405f444a8a43435a88974c951709"},
	{160, "129958033e1e9de64dad26ecf4515bf2"},
	{239, "c0fcaff55e3250d37c6b5b38b860e030"},
	{240, "37c9c30b79a63321664d50576f94a6ef"},
	{241, "adc4079f1717179a1271607fad465dce"},
	{255, "2b1213e92930370af6d8c8f6d1d3b555"},
	{256, "d3932ddd6d35be992339a60164486ee7"},
	{257, "65e218743146a366bd1ae854de420175"},
	{500, "af0ab799f1e4d1592905e046a9641cfe"},
	{1023, "40894b54b506980333e9ea7fe1db4b77"},
	{1024, "55658b0da0d4cef8eda4c557de42db9d"},
	{1025, "b7831fda2869ad2f0048f429ec4089bf"},
	{4096, "532912e5d927a0fbc407cd4f120a401a"},
	{8191, "0fd48cf7b37784b6fad6f2335f444f7f"},
	{8192, "3e57d113e339c00ffee80cb3a07c9d49"},
	{8193, "4138a52316b3b7173bd5efb11e5d0776"},
	{100000, "51956933eb44503092daf14dd672ce3d"},
}

func TestHash128Reference(t *testing.T) {
	for _, v := range vectors {
		h := Hash128(input(v.n))
		b := h.Bytes()
		if got := hex.EncodeToString(b[:]); got != v.want {
			t.Errorf("len %d: got %s want %s", v.n, got, v.want)
		}
		if hs := HashString128(string(input(v.n))); hs != h {
			t.Errorf("len %d: HashString128 %v != Hash128 %v", v.n, hs, h)
		}
	}
}

// Distinct inputs must not collapse: the digest depends on every byte and on the
// length. A failure means a code path is ignoring part of its input. Equal inputs are
// skipped (empty and 1-byte messages repeat by construction).
func TestHash128Sensitive(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	seen := map[Uint128]string{}
	for n := 0; n < 300; n++ {
		for k := 0; k < 50; k++ {
			b := make([]byte, n)
			r.Read(b)
			h := Hash128(b)
			if prev, ok := seen[h]; ok && prev != string(b) {
				t.Fatalf("distinct inputs (len %d and %d) share digest %v", len(prev), n, h)
			}
			seen[h] = string(b)
		}
	}
}

func TestHash128NoAlloc(t *testing.T) {
	in := input(30)
	if a := testing.AllocsPerRun(100, func() { _ = Hash128(in) }); a != 0 {
		t.Errorf("Hash128 allocates %v times per call", a)
	}
}
