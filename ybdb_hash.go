package gocql

import (
	"fmt"
	"strconv"
)

func getByte(k []byte, pos int) int64 {
	return int64(k[pos]) & int64(0xff)
}

func getLong(k []byte, pos int) int64 {
	return (getByte(k, pos) |
		getByte(k, pos+1)<<8 |
		getByte(k, pos+2)<<16 |
		getByte(k, pos+3)<<24 |
		getByte(k, pos+4)<<32 |
		getByte(k, pos+5)<<40 |
		getByte(k, pos+6)<<48 |
		getByte(k, pos+7)<<56)
}

func unsignedRightShift(n int64, p int64) int64 {
	// todo assert p <= 64

	// fmt.Printf("Inside unsignedRightShift() n, p: %d, %d\n", n, p)
	i := p
	r := n >> p
	// fmt.Printf("Done n >> p \n")
	var mask string = "0B"
	for j := 64; j > 0; j-- {
		if i > 0 {
			mask += "0"
		} else {
			mask += "1"
		}
		i--
	}

	// fmt.Printf("unsignedRightShift() mask: %s", mask)

	var maskN, err = strconv.ParseInt(mask, 0, 64)
	if err != nil {
		fmt.Printf("unsignedRightShift() threw error: %v", err)
		// return -1, err
	}
	r = r & maskN
	return r
}

func hash64(k []byte, s int64) int64 {
	// Set up the internal state
	fmt.Println("Inside hash64() ...")
	var ss uint64 = 0xe08c1d668b756f82 // the golden ratio an arbitrary value
	var a int64 = int64(ss)            // equivalent to -2266404186210603134
	var b int64 = int64(ss)
	var c int64 = s // variable initialization of internal state

	var pos int = 0

	// handle most of the key
	for len(k)-pos >= 24 {
		a += getLong(k, pos)
		pos += 8
		b += getLong(k, pos)
		pos += 8
		c += getLong(k, pos)
		pos += 8

		// mix64(a, b, c)
		a -= b
		a -= c
		a ^= unsignedRightShift(c, 43)
		b -= c
		b -= a
		b ^= (a << 9)
		c -= a
		c -= b
		c ^= unsignedRightShift(b, 8)
		a -= b
		a -= c
		a ^= unsignedRightShift(c, 38)
		b -= c
		b -= a
		b ^= (a << 23)
		c -= a
		c -= b
		c ^= unsignedRightShift(b, 5)
		a -= b
		a -= c
		a ^= unsignedRightShift(c, 35)
		b -= c
		b -= a
		b ^= (a << 49)
		c -= a
		c -= b
		c ^= unsignedRightShift(b, 11)
		a -= b
		a -= c
		a ^= unsignedRightShift(c, 12)
		b -= c
		b -= a
		b ^= (a << 18)
		c -= a
		c -= b
		c ^= unsignedRightShift(b, 22)
	}

	// handle the last 23 bytes
	c += int64(len(k))
	switch len(k) - pos { // all the case statements fall through
	case 23:
		c += getByte(k, pos+22) << 56
		fallthrough
	case 22:
		c += getByte(k, pos+21) << 48
		fallthrough
	case 21:
		c += getByte(k, pos+20) << 40
		fallthrough
	case 20:
		c += getByte(k, pos+19) << 32
		fallthrough
	case 19:
		c += getByte(k, pos+18) << 24
		fallthrough
	case 18:
		c += getByte(k, pos+17) << 16
		fallthrough
	case 17:
		c += getByte(k, pos+16) << 8
		fallthrough
		// the first byte of c is reserved for the length
	case 16:
		b += getLong(k, pos+8)
		a += getLong(k, pos)
		break // special handling
	case 15:
		b += getByte(k, pos+14) << 48
		fallthrough
	case 14:
		b += getByte(k, pos+13) << 40
		fallthrough
	case 13:
		b += getByte(k, pos+12) << 32
		fallthrough
	case 12:
		b += getByte(k, pos+11) << 24
		fallthrough
	case 11:
		b += getByte(k, pos+10) << 16
		fallthrough
	case 10:
		b += getByte(k, pos+9) << 8
		fallthrough
	case 9:
		b += getByte(k, pos+8)
		fallthrough
	case 8:
		a += getLong(k, pos)
		break // special handling
	case 7:
		a += getByte(k, pos+6) << 48
		fallthrough
	case 6:
		a += getByte(k, pos+5) << 40
		fallthrough
	case 5:
		a += getByte(k, pos+4) << 32
		fallthrough
	case 4:
		a += getByte(k, pos+3) << 24
		fallthrough
	case 3:
		a += getByte(k, pos+2) << 16
		fallthrough
	case 2:
		a += getByte(k, pos+1) << 8
		fallthrough
	case 1:
		a += getByte(k, pos)
		// case 0: nothing left to add
	}

	// mix64(a, b, c)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 43)
	b -= c
	b -= a
	b ^= (a << 9)
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 8)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 38)
	b -= c
	b -= a
	b ^= (a << 23)
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 5)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 35)
	b -= c
	b -= a
	b ^= (a << 49)
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 11)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 12)
	b -= c
	b -= a
	b ^= (a << 18)
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 22)

	return c
}

func GetKey(b []byte) int64 {
	var SEED int64 = 97
	var h int64 = hash64(b, SEED)
	var h1 int64 = unsignedRightShift(h, 48)
	var h2 int64 = 3 * unsignedRightShift(h, 32)
	var h3 int64 = 5 * unsignedRightShift(h, 16)
	var h4 int64 = 7 * (h & 0xffff)
	return (int64)((h1 ^ h2 ^ h3 ^ h4) & 0xffff)
}

func getKeyFromStatement() int64 {
	return GetKey(nil)
}
