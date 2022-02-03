package parser

import (
	"testing"
)

func Test_ReplaceRandom(t *testing.T) {
	f := func() uint64 {
		return 1234
	}

	tests := []struct {
		in  string
		out string
	}{
		{
			in:  "",
			out: "",
		},
		{
			in:  "SELECT",
			out: "SELECT",
		},
		{
			in:  "SELECT FROM foo WHERE x=random()",
			out: "SELECT FROM foo WHERE x=1234",
		},
		{
			in: `SELECT FROM foo
WHERE x=random()`,
			out: `SELECT FROM foo
WHERE x=1234`,
		},
		{
			in:  "SELECT FROM foo WHERE x=random() AND y=random()",
			out: "SELECT FROM foo WHERE x=1234 AND y=1234",
		},
		// {
		// 	in:  `SELECT FROM foo WHERE x="random()`,
		// 	out: `SELECT FROM foo WHERE x="random()`,
		// },
		// {
		// 	in:  `SELECT FROM foo WHERE x="random()"`,
		// 	out: `SELECT FROM foo WHERE x="random()"`,
		// },
		// {
		// 	in:  `SELECT FROM foo WHERE x='random()'`,
		// 	out: `SELECT FROM foo WHERE x='random()'`,
		// },
		// {
		// 	in:  `SELECT FROM foo WHERE x='"random()"'`,
		// 	out: `SELECT FROM foo WHERE x='"random()"'`,
		// },
		// {
		// 	in:  `SELECT FROM foo WHERE x="'random()'"`,
		// 	out: `SELECT FROM foo WHERE x="'random()'",
		// },
		// {
		// 	in:  `SELECT FROM foo WHERE x="\"random()"`,
		// 	out: `SELECT FROM foo WHERE x="\"random()"`,
		// },
		// {
		// 	in:  "SELECT FROM foo WHERE x=RAnDOM()",
		// 	out: "SELECT FROM foo WHERE x=1234",
		// },
		// {
		// 	in:  "SELECT FROM foo WHERE x=RAnDOM() AND y=999",
		// 	out: "SELECT FROM foo WHERE x=1234 AND y=999",
		// },
		// {
		// 	in:  "SELECT FROM foo WHERE x=RAnDOM()",
		// 	out: "SELECT FROM foo WHERE x=1234",
		// },
		// {
		//	int: `SELECT column_1 FROM table_1 WHERE column_1 = (SELECT column_1 FROM table_2 WHERE column1=random())`,
		// 	out: `SELECT column_1 FROM table_1 WHERE column_1 = (SELECT column_1 FROM table_2 WHERE column1=1234)`,
		// }
	}

	for i, tt := range tests {
		got := ReplaceRandom(tt.in, f)
		if got != tt.out {
			t.Fatalf("random replace failed for test %d, got: %s, exp: %s", i, got, tt.out)
		}
	}
}
