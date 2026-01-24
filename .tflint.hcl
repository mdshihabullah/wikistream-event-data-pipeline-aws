config {
  call_module_type = "local"
  force = false
  disabled_by_default = false

  plugin_dir = "~/.tflint.d/plugins"
}

plugin "aws" {
  enabled = true
  version = "0.32.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

rule "terraform_comment_syntax" {
  enabled = false
}

rule "terraform_required_providers" {
  enabled = false
}

rule "terraform_required_version" {
  enabled = false
}

rule "terraform_naming_convention" {
  enabled = false
}

rule "terraform_unused_declarations" {
  enabled = false
}
