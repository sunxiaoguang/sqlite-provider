#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatementSplit {
    pub(crate) range: Option<(usize, usize)>,
    pub(crate) tail: usize,
    pub(crate) terminated: bool,
    pub(crate) complete: bool,
}

fn skip_leading_ws_and_comments(sql: &[u8], mut i: usize) -> (usize, bool) {
    while i < sql.len() {
        while i < sql.len() && sql[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < sql.len() && sql[i] == b'-' && sql[i + 1] == b'-' {
            i += 2;
            while i < sql.len() && sql[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < sql.len() && sql[i] == b'/' && sql[i + 1] == b'*' {
            i += 2;
            let mut closed = false;
            while i + 1 < sql.len() {
                if sql[i] == b'*' && sql[i + 1] == b'/' {
                    i += 2;
                    closed = true;
                    break;
                }
                i += 1;
            }
            if !closed {
                return (sql.len(), false);
            }
            continue;
        }
        break;
    }
    (i, true)
}

fn is_ident_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_'
}

fn is_ident_continue(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

fn token_eq(token: &[u8], word: &[u8]) -> bool {
    token.eq_ignore_ascii_case(word)
}

pub(crate) fn split_first_statement(sql: &[u8], offset: usize) -> StatementSplit {
    let mut scan = offset;
    let mut saw_leading_delimiter = false;
    let start = loop {
        let (start, complete_prefix) = skip_leading_ws_and_comments(sql, scan);
        if !complete_prefix {
            return StatementSplit {
                range: None,
                tail: sql.len(),
                terminated: false,
                complete: false,
            };
        }
        if start >= sql.len() {
            return StatementSplit {
                range: None,
                tail: sql.len(),
                terminated: saw_leading_delimiter,
                complete: true,
            };
        }
        if sql[start] == b';' {
            saw_leading_delimiter = true;
            scan = start + 1;
            continue;
        }
        break start;
    };

    let mut i = start;
    let mut saw_create = false;
    let mut saw_trigger = false;
    let mut trigger_depth = 0usize;
    let mut case_depth = 0usize;
    let mut trigger_needs_terminator = false;
    while i < sql.len() {
        if trigger_needs_terminator {
            match sql[i] {
                ch if ch.is_ascii_whitespace() => {
                    i += 1;
                    continue;
                }
                b'-' if i + 1 < sql.len() && sql[i + 1] == b'-' => {
                    i += 2;
                    while i < sql.len() && sql[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                b'/' if i + 1 < sql.len() && sql[i + 1] == b'*' => {
                    i += 2;
                    let mut closed = false;
                    while i + 1 < sql.len() {
                        if sql[i] == b'*' && sql[i + 1] == b'/' {
                            i += 2;
                            closed = true;
                            break;
                        }
                        i += 1;
                    }
                    if !closed {
                        return StatementSplit {
                            range: Some((start, sql.len())),
                            tail: sql.len(),
                            terminated: false,
                            complete: false,
                        };
                    }
                    continue;
                }
                b';' => {
                    i += 1;
                    return StatementSplit {
                        range: Some((start, i)),
                        tail: i,
                        terminated: true,
                        complete: true,
                    };
                }
                _ => {
                    return StatementSplit {
                        range: Some((start, sql.len())),
                        tail: sql.len(),
                        terminated: false,
                        complete: false,
                    };
                }
            }
        }
        match sql[i] {
            b'\'' => {
                i += 1;
                let mut closed = false;
                while i < sql.len() {
                    if sql[i] == b'\'' {
                        i += 1;
                        if i < sql.len() && sql[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return StatementSplit {
                        range: Some((start, sql.len())),
                        tail: sql.len(),
                        terminated: false,
                        complete: false,
                    };
                }
            }
            b'"' => {
                i += 1;
                let mut closed = false;
                while i < sql.len() {
                    if sql[i] == b'"' {
                        i += 1;
                        if i < sql.len() && sql[i] == b'"' {
                            i += 1;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return StatementSplit {
                        range: Some((start, sql.len())),
                        tail: sql.len(),
                        terminated: false,
                        complete: false,
                    };
                }
            }
            b'`' => {
                i += 1;
                let mut closed = false;
                while i < sql.len() {
                    if sql[i] == b'`' {
                        i += 1;
                        if i < sql.len() && sql[i] == b'`' {
                            i += 1;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return StatementSplit {
                        range: Some((start, sql.len())),
                        tail: sql.len(),
                        terminated: false,
                        complete: false,
                    };
                }
            }
            b'[' => {
                i += 1;
                while i < sql.len() && sql[i] != b']' {
                    i += 1;
                }
                if i >= sql.len() {
                    return StatementSplit {
                        range: Some((start, sql.len())),
                        tail: sql.len(),
                        terminated: false,
                        complete: false,
                    };
                }
                i += 1;
            }
            b'-' if i + 1 < sql.len() && sql[i + 1] == b'-' => {
                i += 2;
                while i < sql.len() && sql[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < sql.len() && sql[i + 1] == b'*' => {
                i += 2;
                let mut closed = false;
                while i + 1 < sql.len() {
                    if sql[i] == b'*' && sql[i + 1] == b'/' {
                        i += 2;
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return StatementSplit {
                        range: Some((start, sql.len())),
                        tail: sql.len(),
                        terminated: false,
                        complete: false,
                    };
                }
            }
            ch if is_ident_start(ch) => {
                let token_start = i;
                i += 1;
                while i < sql.len() && is_ident_continue(sql[i]) {
                    i += 1;
                }
                let token = &sql[token_start..i];
                if trigger_depth > 0 {
                    if token_eq(token, b"case") {
                        case_depth += 1;
                    } else if token_eq(token, b"begin") {
                        trigger_depth += 1;
                    } else if token_eq(token, b"end") {
                        if case_depth > 0 {
                            case_depth -= 1;
                        } else if trigger_depth > 0 {
                            trigger_depth -= 1;
                            if trigger_depth == 0 {
                                trigger_needs_terminator = true;
                            }
                        }
                    }
                    continue;
                }
                if !saw_create {
                    saw_create = token_eq(token, b"create");
                    continue;
                }
                if !saw_trigger {
                    if token_eq(token, b"trigger") {
                        saw_trigger = true;
                    } else if !token_eq(token, b"temp")
                        && !token_eq(token, b"temporary")
                        && !token_eq(token, b"or")
                        && !token_eq(token, b"replace")
                        && !token_eq(token, b"if")
                        && !token_eq(token, b"not")
                        && !token_eq(token, b"exists")
                    {
                        saw_create = false;
                    }
                    continue;
                }
                if token_eq(token, b"begin") {
                    trigger_depth = 1;
                }
            }
            b';' => {
                if trigger_depth > 0 {
                    i += 1;
                    continue;
                }
                i += 1;
                return StatementSplit {
                    range: Some((start, i)),
                    tail: i,
                    terminated: true,
                    complete: true,
                };
            }
            _ => i += 1,
        }
    }

    StatementSplit {
        range: Some((start, sql.len())),
        tail: sql.len(),
        terminated: false,
        complete: !trigger_needs_terminator,
    }
}
