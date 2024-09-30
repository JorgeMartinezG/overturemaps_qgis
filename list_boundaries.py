from utils import get_boundaries


def main():
    boundaries = sorted(
        get_boundaries("", with_geom=False), key=lambda x: x.name
    )

    for boundary in boundaries:
        print(f"{boundary.name} - {boundary.iso3} - {boundary.id}")


if __name__ == "__main__":
    main()
