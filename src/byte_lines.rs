use std::io::{self, BufRead};

/// Iterator that yields lines as `Vec<u8>`. Similar to `std::io::Lines` but for
/// raw bytes.
pub struct ByteLines<R: BufRead> {
    reader: R,
    buffer: Vec<u8>,
}

impl<R: BufRead> ByteLines<R> {
    pub fn new(reader: R) -> Self {
        ByteLines {
            reader,
            buffer: Vec::new(),
        }
    }
}

impl<R: BufRead> Iterator for ByteLines<R> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buffer.clear();
        match self.reader.read_until(b'\n', &mut self.buffer) {
            Ok(0) => None,                          // End of file
            Ok(_) => Some(Ok(self.buffer.clone())), // Clone the buffer to return the line
            Err(e) => Some(Err(e)),
        }
    }
}

/* TODO: add test cases
fn main() -> io::Result<()> {
    let file_path = "example.txt";
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    // Create the ByteLines iterator
    let byte_lines = ByteLines::new(reader);

    // Iterate over lines in bytes
    for (line_number, line) in byte_lines.enumerate() {
        match line {
            Ok(bytes) => {
                println!("Line {} (bytes): {:?}", line_number + 1, bytes);
                // Optionally, convert to string for verification
                if let Ok(line_str) = String::from_utf8(bytes.clone()) {
                    println!("Line {} (as string): {}", line_number + 1, line_str);
                }
            }
            Err(e) => eprintln!("Error reading line: {}", e),
        }
    }

    Ok(())
}
*/

/* Super imperative code to seek to a specific line in file buffer
 * ...
use std::fs::File;
use std::io::{self, BufRead, BufReader};

fn read_line_from_file_as_bytes(file_path: &str, line_number: usize) -> io::Result<Vec<u8>> {
    // Open the file for reading.
    let file = File::open(file_path)?;

    // Create a buffered reader for efficient reading.
    let mut reader = BufReader::new(file);

    // Buffer to store the line's raw bytes.
    let mut line = Vec::new();

    // Iterate over the lines until we reach the desired line number.
    for current_line in 0..line_number {
        line.clear(); // Clear the buffer for the next line.
        let bytes_read = reader.read_until(b'\n', &mut line)?; // Read raw bytes until newline.

        if bytes_read == 0 {
            // If we reach the end of the file before finding the desired line.
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Line number out of range"));
        }

        if current_line + 1 == line_number {
            // Return the line when we reach the desired line number.
            return Ok(line);
        }
    }

    // If the line number is out of range, return an error.
    Err(io::Error::new(io::ErrorKind::InvalidInput, "Line number out of range"))
}

fn main() {
    let file_path = "example.txt";
    let line_number = 3;

    match read_line_from_file_as_bytes(file_path, line_number) {
        Ok(line) => {
            println!("Line {} (bytes): {:?}", line_number, line);
            // Optionally, print the line as a string for verification.
            if let Ok(line_str) = String::from_utf8(line.clone()) {
                println!("Line {} (as string): {}", line_number, line_str);
            }
        },
        Err(e) => println!("Error: {}", e),
    }
}
*/
