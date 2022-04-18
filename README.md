# StepView

Visualize a summary of all your stepfunctions in the terminal for
multiple AWS profiles.

![stepview](./assets/stepview.png)

## Installation

    pip install stepview

## Usage

    stepview git:(main) ✗ stepview --help

        Usage: stepview [OPTIONS]

        Options:
          --profile TEXT                  specify the aws profile you want to use as a
                                          comma seperated string. For example '--
                                          profile profile1,profile2,profile3,...'
                                          [required]
          --period TEXT                   specify the time period for which you wish
                                          to look back. You can choose from the
                                          values: "minute, hour, today, day, week,
                                          month, year"   [default: day]
          --help                          Show this message and exit.

## Example

    stepview --profile default,some-profile --period year
