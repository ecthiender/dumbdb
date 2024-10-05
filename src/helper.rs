use std::{fs::File, io::Write};

use anyhow::Context;

pub fn write_to_file(file: &mut File, data: String) -> anyhow::Result<()> {
    writeln!(file, "{}", data)
        .with_context(|| "FATAL: Internal Error: Failed writing data to file")?;
    let _ = file.flush();
    Ok(())
}
