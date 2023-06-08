from cam.compound_naming import get_compound_name_object, template_address


def main() -> None:
    sparql_result = get_compound_name_object(
        "https://linked.data.gov.au/dataset/qld-addr/addr-1075435",
        "http://localhost:7200/repositories/addressing",
    )

    result = template_address(sparql_result)

    print(result)


if __name__ == "__main__":
    main()
