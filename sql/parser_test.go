package sql

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func Test_stackEmpty(t *testing.T) {
	s := newStack()
	if !s.empty() {
		t.Fatal("new stack is not empty")
	}

	if s.peek() != rune(0) {
		t.Fatal("peek of empty stack does not return correct value")
	}
}

func Test_stackSingle(t *testing.T) {
	s := newStack()
	s.push('x')
	if s.empty() {
		t.Fatal("non-empty stack marked as empty")
	}

	if s.peek() != 'x' {
		t.Fatal("peek of stack with single entry does not return correct value")
	}

	if s.pop() != 'x' {
		t.Fatal("pop of stack with single entry does not return correct value")
	}

	if !s.empty() {
		t.Fatal("popped stack is not empty")
	}
}

func Test_stackMulti(t *testing.T) {
	s := newStack()
	s.push('x')
	s.push('y')
	s.push('z')

	if s.pop() != 'z' {
		t.Fatal("pop of 1st multi stack does not return correct value")
	}
	if s.pop() != 'y' {
		t.Fatal("pop of 2nd multi stack does not return correct value")
	}
	if s.pop() != 'x' {
		t.Fatal("pop of 3rd multi stack does not return correct value")
	}

	if !s.empty() {
		t.Fatal("popped mstack is not empty")
	}
}

func Test_ScannerNew(t *testing.T) {
	s := NewScanner(nil)
	if s == nil {
		t.Fatalf("failed to create basic Scanner")
	}
}

func Test_ScannerEmpty(t *testing.T) {
	r := bytes.NewBufferString("")
	s := NewScanner(r)

	_, err := s.Scan()
	if err != io.EOF {
		t.Fatal("Scan of empty string did not return EOF")
	}
}

func Test_ScannerSemi(t *testing.T) {
	r := bytes.NewBufferString(";")
	s := NewScanner(r)

	l, err := s.Scan()
	if err != nil {
		t.Fatal("Scan of single semicolon failed")
	}
	if l != "" {
		t.Fatal("Scan of single semicolon returned incorrect value")
	}
	_, err = s.Scan()
	if err != io.EOF {
		t.Fatal("Scan of empty string after semicolon did not return EOF")
	}
}

func Test_ScannerSingleStatement(t *testing.T) {
	r := bytes.NewBufferString("SELECT * FROM foo;")
	s := NewScanner(r)

	l, err := s.Scan()
	if err != nil {
		t.Fatal("Scan of single statement failed")
	}
	if l != "SELECT * FROM foo" {
		t.Fatal("Scan of single statement returned incorrect value")
	}
	_, err = s.Scan()
	if err != io.EOF {
		t.Fatal("Scan of empty string after statement did not return EOF")
	}
}

func Test_ScannerSingleStatementQuotes(t *testing.T) {
	r := bytes.NewBufferString(`SELECT * FROM "foo";`)
	s := NewScanner(r)

	l, err := s.Scan()
	if err != nil {
		t.Fatal("Scan of single statement failed")
	}
	if l != `SELECT * FROM "foo"` {
		t.Fatal("Scan of single statement returned incorrect value")
	}
	_, err = s.Scan()
	if err != io.EOF {
		t.Fatal("Scan of empty string after statement did not return EOF")
	}
}

func Test_ScannerSingleStatementQuotesEmbedded(t *testing.T) {
	r := bytes.NewBufferString(`SELECT * FROM ";SELECT * FROM '"foo"'";`)
	s := NewScanner(r)

	l, err := s.Scan()
	if err != nil {
		t.Fatal("Scan of single statement failed")
	}
	if l != `SELECT * FROM ";SELECT * FROM '"foo"'"` {
		t.Fatal("Scan of single statement returned incorrect value")
	}
	_, err = s.Scan()
	if err != io.EOF {
		t.Fatal("Scan of empty string after statement did not return EOF")
	}
}

func Test_ScannerMultiStatement(t *testing.T) {
	e := []string{`SELECT * FROM foo;`, `SELECT * FROM bar;`}
	r := bytes.NewBufferString(strings.Join(e, ""))
	s := NewScanner(r)

	for i := range e {
		l, err := s.Scan()
		if err != nil {
			t.Fatal("Scan of multi statement failed")
		}

		if l != strings.Trim(e[i], "\n;") {
			t.Fatalf("Scan of multi statement returned incorrect value, exp %s, got %s", e[i], l)
		}
	}
}

func Test_ScannerMultiStatementQuotesEmbedded(t *testing.T) {
	e := []string{`SELECT * FROM "foo;barx";`, `SELECT * FROM bar;`}
	r := bytes.NewBufferString(strings.Join(e, ""))
	s := NewScanner(r)

	for i := range e {
		l, err := s.Scan()
		if err != nil {
			t.Fatal("Scan of multi statement failed")
		}

		if l != strings.Trim(e[i], "\n;") {
			t.Fatalf("Scan of multi statement returned incorrect value, exp %s, got %s", e[i], l)
		}
	}
}

// XX I am missing this case: '"' ????

func Test_ScannerMultiLine(t *testing.T) {
	stmt := `CREATE TABLE [Customer]
(
    [CustomerId] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    [FirstName] NVARCHAR(40)  NOT NULL,
    [LastName] NVARCHAR(20)  NOT NULL,
    [Company] NVARCHAR(80),
    [Address] NVARCHAR(70),
    [City] NVARCHAR(40),
    [State] NVARCHAR(40),
    [Country] NVARCHAR(40),
    [PostalCode] NVARCHAR(10),
    [Phone] NVARCHAR(24),
    [Fax] NVARCHAR(24),
    [Email] NVARCHAR(60)  NOT NULL,
    [SupportRepId] INTEGER,
    FOREIGN KEY ([SupportRepId]) REFERENCES [Employee] ([EmployeeId])
                ON DELETE NO ACTION ON UPDATE NO ACTION
);`
	r := bytes.NewBufferString(stmt)
	s := NewScanner(r)

	l, err := s.Scan()
	if err != nil {
		t.Fatal("Scan of multiline statement failed")
	}
	if l != strings.Trim(stmt, "\n;") {
		t.Fatal("Scan of multiline statement returned incorrect value")
	}
	_, err = s.Scan()
	if err != io.EOF {
		t.Fatal("Scan of empty string after statement did not return EOF")
	}
}
