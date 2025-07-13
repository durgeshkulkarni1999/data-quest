#!/usr/bin/env python3
#!/usr/bin/env python3
import aws_cdk as cdk
from pipeline_stack import PipelineStack

app = cdk.App()

# No `env=`: uses whatever AWS CLI/SDK default account & region you’ve configured
PipelineStack(app, "DataPipelineStack")

app.synth()

