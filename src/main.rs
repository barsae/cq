use std::path::Path;
use std::fs::File;
use std::io::{Read, stdin};
use std::env::args;

// test cases
//   read from file: cq -in <file>
//   read from stdin by default: cq
//   read a column: cq -select city
//   read columns: cq -select city county
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

struct ReaderArgs {
    input: Box<dyn Read>,
    columns: Vec<String>,
    filters: Vec<Filter>,
}

fn main() -> Result<(), String> {
    let args: Vec<String> = args().collect();
    let mut reader_args = parse_args(Iterator::new(args))?;
    let mut column_indexes = vec!();
    let mut in_header = true;
    let mut column_index = 0;
    let mut current_value = vec!();
    let mut buf = vec!(0, 0, 0);
    let mut printed = false;

    let mut len = reader_args.input.read(&mut buf).unwrap();
    while len > 0 {
        len = reader_args.input.read(&mut buf).unwrap();
        for i in 0 .. len {
            match buf[i] {
                10 => {
                    // Truly ignore
                }
                13 => {
                    in_header = false;
                    column_index = 0;
                    current_value = vec!();
                    if printed {
                        println!();
                        printed = false;
                    }
                }
                44 => {
                    if in_header {
                        let value = String::from_utf8(current_value).unwrap();
                        if reader_args.columns.contains(&value) {
                            column_indexes.push(column_index);
                        }
                    } else if column_indexes.contains(&column_index) || reader_args.columns.len() == 0 {
                        if printed {
                            print!(",");
                        }
                        let value = String::from_utf8(current_value).unwrap();
                        print!("{}", value);
                        printed = true;
                    }

                    column_index += 1;
                    current_value = vec!();
                },
                _ => {
                    current_value.push(buf[i])
                }
            }
        }
    }

    Ok(())
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
            "-input" => {
                let filename = args.expect("Expected filename after -input".to_string())?;
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
