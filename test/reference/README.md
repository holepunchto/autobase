# Reference Linearizer

## Testing

### Fuzzing

`npm run fuzz <directory>`

Writes failing test cases to `fuzz/<directory>/<label>.js` with a randomy generated `label`.

If no `directory` is passed, `generated` will be used by default.

Fuzzing parameters are defined in `fuzz/fuzz.js`

### Individual case

`node fuzz/case <label> <directory>`

Will test the `label` case individually. Useful for checking execution details.

Note: this will only roll back a single time, so a failing test may sometimes pass.

### All cases

`node fuzz/generated <directory>` 

Run all generated tests in a directory.

Loops over each test 1000 times internally for higher confidence.