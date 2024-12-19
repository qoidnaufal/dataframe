use dataframe::{DataFrame, Error};
 
fn main() -> Result<(), Error> {
    let df = DataFrame::read_csv("examples/ppda.csv")?;
    println!("{:?}", df);

    Ok(())
}
