// The MIT License (MIT)

// Copyright (c) 2014-2017 Philip O'Toole

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

/*
Package rqlite is a lightweight, distributed relational database built on SQLite. rqlite uses Raft to achieve consensus across the cluster of SQLite databases. It ensures that every change made to the database is made to a majority of underlying SQLite files, or none at all.

rqlite gives you the functionality of a rock solid, fault-tolerant, replicated relational database, but with very easy installation, deployment, and operation. With it you've got a lightweight and reliable distributed store for relational data. You could use rqlite as part of a larger system, as a central store for some critical relational data, without having to run a heavier solution like MySQL.
*/
package rqlite
