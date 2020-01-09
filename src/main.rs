use std::path::Path;
use std::fs::File;
use std::io::{Read, stdin};
use std::env::args;

// test cases
//   .read from file: cq -in <file>
//   .read from stdin by default: cq
//   .read a column: cq -select city
//   .read columns: cq -select city county
//   read rows: cq -where state -eq WA
//   read column(s) from a row: cq -c city county -where state -eq WA
//   change delimiter: cq -delim "|"
//   tbd: inserts, updates, deletes
//   tbd: quotes, line separator, escaping
//   tbd: header vs index
//   tbd: encoding other than ut8

struct Iterator<T> {
    items: Vec<T>,
    index: usize,
}

impl <T> Iterator<T> {
    fn new(items: Vec<T>) -> Iterator<T> {
        Iterator {
            index: 0,
            items,
        }
    }

    fn accept(&mut self) -> Option<&T> {
        if self.index < self.items.len() {
            self.index += 1;
            self.items.get(self.index - 1)
        } else {
            None
        }
    }

    fn refund(&mut self) {
        self.index -= 1;
    }

    fn expect(&mut self, err: String) -> Result<&T, String> {
        self.accept().ok_or(err)
    }
}

struct Filter {
    column: String,
    value: String,
}

struct FilterState {
    column_index: usize,
    value: String,
    matched: bool,
}

struct ReaderArgs {
    input: Box<dyn Read>,
    columns: Vec<String>,
    filters: Vec<Filter>,
}

struct ReaderState {
    column_indexes: Vec<usize>,
    filters: Vec<FilterState>,
    in_header: bool,
    column_index: usize,
    current_value: Vec<u8>,
    buf: Vec<u8>,
    to_print: Vec<String>,
}

fn main() -> Result<(), String> {
    let cmd_args: Vec<String> = args().collect();
    let mut args = parse_args(Iterator::new(cmd_args))?;
    let mut state = ReaderState {
        column_indexes: vec!(),
        filters: vec!(),
        in_header: true,
        column_index: 0,
        current_value: vec!(),
        buf: vec!(0, 0, 0),
        to_print: vec!(),
    };

    let mut len = args.input.read(&mut state.buf).unwrap();
    while len > 0 {
        for i in 0 .. len {
            match state.buf[i] {
                10 => {
                    // Truly ignore
                }
                13 => {
                    handle_value_end(&args, &mut state);
                    handle_line_end(&args, &mut state);
                }
                44 => {
                    handle_value_end(&args, &mut state);
                },
                _ => {
                    state.current_value.push(state.buf[i])
                }
            }
        }

        len = args.input.read(&mut state.buf).unwrap();
    }
    handle_value_end(&args, &mut state);
    handle_line_end(&args, &mut state);

    Ok(())
}

fn handle_line_end(_args: &ReaderArgs, state: &mut ReaderState) {
    state.in_header = false;
    state.column_index = 0;
    state.current_value = vec!();

    if state.filters.iter().all(|f| f.matched) {
        if state.to_print.len() > 0 {
            let mut first = true;
            for value in state.to_print.iter() {
                if !first {
                    print!(",");
                }
                first = false;
                print!("{}", value);
            }

            state.to_print.clear();
            println!();
        }
    }

    for filter in state.filters.iter_mut() {
        filter.matched = false;
    }
}

fn handle_value_end(args: &ReaderArgs, state: &mut ReaderState) {
    if state.in_header {
        let value = std::str::from_utf8(state.current_value.as_slice()).unwrap();
        if args.columns.contains(&value.to_string()) {
            state.column_indexes.push(state.column_index);
        }
        if let Some(filter) = args.filters.iter().find(|f| &f.column == value) {
            state.filters.push(FilterState {
                column_index: state.column_index,
                value: filter.value.to_string(),
                matched: false,
            });
        }
    } else {
        if state.column_indexes.contains(&state.column_index) || args.columns.len() == 0 {
            let value = String::from_utf8(state.current_value.clone()).unwrap();
            state.to_print.push(value);
        }

        let column_index = state.column_index.clone();
        let filter_maybe =
            state.filters.iter_mut().find(|f| f.column_index == column_index);
        if let Some(filter) = filter_maybe {
            let value = String::from_utf8(state.current_value.clone()).unwrap();
            if value == filter.value {
                filter.matched = true;
            }
        }
    }

    state.column_index += 1;
    state.current_value = vec!();
}

fn parse_args(mut args: Iterator<String>) -> Result<ReaderArgs, String> {
    args.expect("First argument should be the executable path".to_string())?;

    let mut result = ReaderArgs {
        input: Box::new(stdin()),
        columns: vec!(),
        filters: vec!(),
    };

    while let Some(arg) = args.accept() {
        match arg.as_str() {
            "-in" => {
                let filename = args.expect("Expected filename after -in".to_string())?;
                let path = Path::new(filename);
                result.input = Box::new(File::open(&path).unwrap());
            },
            "-select" => {
                while let Some(select) = args.accept() {
                    if select.starts_with("-") {
                        args.refund();
                        break;
                    } else {
                        result.columns.push(select.to_string());
                    }
                }
            }
            "-where" => {
                let column = args.expect("Expected column name after -where".to_string())?.to_string();
                let eq = args.expect("Expected -eq as part of -where".to_string())?;
                if eq != "-eq" {
                    return Err(format!("Expected -eq, but got '{}'", eq));
                }
                let value = args.expect("Expected value after -eq".to_string())?.to_string();
                result.filters.push(Filter {
                    column,
                    value,
                })
            }
            _ => return Err(format!("Unknown argument '{}'", arg)),
        }
    }

    Ok(result)
}
