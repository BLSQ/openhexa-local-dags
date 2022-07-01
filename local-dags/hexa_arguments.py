import typing


def argument(config_key: str) -> str:
    """Single CLI argument, without operator"""

    return "{{ dag_run.conf['%s'] }}" % config_key


def single(param_name: str, config_key: str, default: str = None) -> str:
    """Single CLI param (--foo=bar)"""

    param_value = "{{ dag_run.conf['%s'] }}" % config_key

    if default is not None:
        return (
            "{%% if '%(config_key)s' in dag_run.conf %%}%(param_name)s %(param_value)s"
            "{%% else %%}%(param_name)s %(default_value)s"
            "{%% endif %%}"
            % {
                "config_key": config_key,
                "param_name": param_name,
                "param_value": param_value,
                "default_value": default,
            }
        )

    return (
        "{%% if '%(config_key)s' in dag_run.conf %%}%(param_name)s %(param_value)s{%% endif %%}"
        % {
            "config_key": config_key,
            "param_name": param_name,
            "param_value": param_value,
        }
    )


def path(param_name: str, config_key: str, *, suffix: typing.Optional[str] = "") -> str:
    """Single CLI param (--foo=bar)"""

    param_value = "{{ dag_run.conf['%(config_key)s']|trim('/') }}%(suffix)s" % {
        "config_key": config_key,
        "suffix": suffix,
    }

    return (
        "{%% if '%(config_key)s' in dag_run.conf %%}%(param_name)s %(param_value)s{%% endif %%}"
        % {
            "config_key": config_key,
            "param_name": param_name,
            "param_value": param_value,
        }
    )


def multi(param_name: str, config_key: str) -> str:
    """Multi-value CLI param (--foo=bar --foo=baz)"""

    loop_content = (
        "{%% for %(loop_key)s in dag_run.conf['%(config_key)s'] %%}"
        "%(param_name)s {{ %(loop_key)s }}{%% if not loop.last %%} {%% endif %%}"
        "{%% endfor %%}"
        % {
            "config_key": config_key,
            "param_name": param_name,
            "loop_key": param_name.lstrip("-"),
        }
    )

    return (
        "{%% if '%(config_key)s' in dag_run.conf %%}%(loop_content)s{%% endif %%}"
        % {
            "config_key": config_key,
            "loop_content": loop_content,
        }
    )


def flag(
    flag_name: typing.Union[str, typing.Sequence[str]],
    config_key: str,
    default: bool,
) -> str:
    """Flag options (--flag or --flag/--no-flag combos)"""

    if default not in (True, False):
        raise ValueError(f"Invalid default for {flag_name}: {default}")

    if isinstance(flag_name, str):
        if default is True:
            return (
                "{%% if '%(config_key)s' in dag_run.conf and dag_run.conf['%(config_key)s'] != false %%}%(flag_name)s"
                "{%% endif %%}"
                % {
                    "config_key": config_key,
                    "flag_name": flag_name,
                }
            )
        else:
            return (
                "{%% if '%(config_key)s' in dag_run.conf and dag_run.conf['%(config_key)s'] == true %%}%(flag_name)s"
                "{%% endif %%}" % {"config_key": config_key, "flag_name": flag_name}
            )
    else:
        on_flag_name, off_flag_name = flag_name
        if default is True:
            return (
                "{%% if '%(config_key)s' in dag_run.conf and dag_run.conf['%(config_key)s'] == false %%}%(off_flag_name)s"
                "{%% else %%}%(on_flag_name)s"
                "{%% endif %%}"
                % {
                    "config_key": config_key,
                    "on_flag_name": on_flag_name,
                    "off_flag_name": off_flag_name,
                }
            )
        else:
            return (
                "{%% if '%(config_key)s' in dag_run.conf and dag_run.conf['%(config_key)s'] == true %%}%(on_flag_name)s"
                "{%% else %%}%(off_flag_name)s"
                "{%% endif %%}"
                % {
                    "config_key": config_key,
                    "on_flag_name": on_flag_name,
                    "off_flag_name": off_flag_name,
                }
            )


assert (
    single("-p", "plop")
    == "{% if 'plop' in dag_run.conf %}-p {{ dag_run.conf['plop'] }}{% endif %}"
)

assert (
    single("--plop", "plop")
    == "{% if 'plop' in dag_run.conf %}--plop {{ dag_run.conf['plop'] }}{% endif %}"
)

assert (
    single("--plop", "plop", default="yop")
    == "{% if 'plop' in dag_run.conf %}--plop {{ dag_run.conf['plop'] }}{% else %}--plop yop{% endif %}"
)

assert (
    path("-p", "plop")
    == "{% if 'plop' in dag_run.conf %}-p {{ dag_run.conf['plop']|trim('/') }}{% endif %}"
)

assert (
    path("-p", "plop", suffix="/yop")
    == "{% if 'plop' in dag_run.conf %}-p {{ dag_run.conf['plop']|trim('/') }}/yop{% endif %}"
)

assert (
    multi("-p", "plop")
    == "{% if 'plop' in dag_run.conf %}{% for p in dag_run.conf['plop'] %}-p {{ p }}{% if not loop.last %} {% endif %}{% endfor %}{% endif %}"
)

assert (
    flag("--flag", "flag", default=True)
    == "{% if 'flag' in dag_run.conf and dag_run.conf['flag'] != false %}--flag{% endif %}"
)

assert (
    flag("--flag", "flag", default=False)
    == "{% if 'flag' in dag_run.conf and dag_run.conf['flag'] == true %}--flag{% endif %}"
)

assert (
    flag(("--flag", "--no-flag"), "flag", default=True)
    == "{% if 'flag' in dag_run.conf and dag_run.conf['flag'] == false %}--no-flag{% else %}--flag{% endif %}"
)

assert (
    flag(("--flag", "--no-flag"), "flag", default=False)
    == "{% if 'flag' in dag_run.conf and dag_run.conf['flag'] == true %}--flag{% else %}--no-flag{% endif %}"
)
