# StepView

Visualize a summary of all your stepfunctions in the terminal for
multiple AWS profiles.

![stepview](./assets/stepview.png)

- See the total executions of your statemachines.
- See the % Succeeded executions.
- See the # of Failed and Running executions.
- Click the statemachine name to navigate to the AWS console.


## Installation

    pip install stepview

## Usage

    stepview --help

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

- [Setup an AWS named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html#cli-configure-profiles-create).
- Choose a time period: `minute`, `hour`, `today`, `day`, `week`, `month`, `year`.

```
stepview --profile foo,bar --period year
```
> **NOTE** This is an alpha version, if you have a lot of statemachines/executions for your period it might crash because you reached the request limit for fetching data from stepfunctions. To solve it for now, use a smaller time period.

<img src="https://public-assets-vincent-claes.s3.eu-west-1.amazonaws.com/stepview/stepview.gif" width="100%">

## Suggestions

- Any suggestions can be shared by creating an [issue](https://github.com/vincentclaes/stepview/issues)
- Or you can reach out to me via [twitter](https://twitter.com/VincentClaes1) or [linkedin](https://www.linkedin.com/in/vincent-claes-0b346337/)
