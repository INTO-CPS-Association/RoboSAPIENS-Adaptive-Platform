"""`python -m rpclpy.skills`, for when the rpio-skills script is not on PATH."""
import sys

from rpclpy.skills import main

sys.exit(main())
